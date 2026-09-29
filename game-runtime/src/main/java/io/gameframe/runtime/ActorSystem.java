package io.gameframe.runtime;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Serial actors with bounded queued work and reserved asynchronous completions. */
public final class ActorSystem implements AutoCloseable {
    public enum FailurePolicy { CONTINUE, STOP }
    public interface Lifecycle {
        default void onStart(ActorRef actor) {}
        default void onFailure(ActorRef actor, Throwable error) {}
        default void onStop(ActorRef actor) {}
    }
    public record ActorOptions(Lifecycle lifecycle, FailurePolicy failurePolicy) {
        public ActorOptions { Objects.requireNonNull(failurePolicy); }
        public static ActorOptions defaults() { return new ActorOptions(null, FailurePolicy.CONTINUE); }
    }
    public record ActorStats(String id, int pending, int capacity, boolean scheduled, boolean stopping,
                             long accepted, long rejected, long processed, long failures,
                             long totalProcessingNanos, Throwable lastFailure) {
        public double averageProcessingNanos() { return processed == 0 ? 0 : (double) totalProcessingNanos / processed; }
        public double load() { return (double) pending / capacity; }
    }
    private static final ThreadLocal<ActorRef> CURRENT = new ThreadLocal<>();
    private final ExecutorService workers;
    private final Map<String, ActorRef> actors = new HashMap<>();
    private final int capacity, maxActors, quantum;
    private boolean closed;

    public ActorSystem(int threads, int maxActors, int capacity, int quantum) {
        if (threads < 1 || maxActors < 1 || capacity < 1 || quantum < 1) throw new IllegalArgumentException();
        this.capacity = capacity; this.maxActors = maxActors; this.quantum = quantum;
        workers = Executors.newFixedThreadPool(threads, r -> new Thread(r, "game-actor"));
    }
    public ActorRef spawn(String id) { return spawn(id, ActorOptions.defaults()); }
    public ActorRef spawn(String id, Lifecycle lifecycle, FailurePolicy policy) {
        return spawn(id, new ActorOptions(lifecycle, policy));
    }
    public synchronized ActorRef spawn(String id, ActorOptions options) {
        Objects.requireNonNull(options);
        if (closed) throw new RejectedExecutionException("actor system closed");
        if (id == null || id.isBlank() || actors.containsKey(id)) throw new IllegalArgumentException("duplicate/empty actor id");
        if (actors.size() >= maxActors) throw new RejectedExecutionException("actor limit");
        var ref = new ActorRef(id, options);
        actors.put(id, ref);
        synchronized (ref) { ref.schedule(); }
        return ref;
    }
    private synchronized List<ActorRef> snapshot() { return List.copyOf(actors.values()); }
    public List<ActorStats> stats() { return snapshot().stream().map(ActorRef::stats).toList(); }
    public List<ActorStats> loadCandidates(int limit) {
        if (limit < 1) throw new IllegalArgumentException("positive limit required");
        return stats().stream().filter(s -> !s.stopping())
            .sorted(Comparator.comparingDouble(ActorStats::load).thenComparing(ActorStats::id)).limit(limit).toList();
    }
    private synchronized void removeActor(ActorRef ref) { actors.remove(ref.id, ref); }

    public final class ActorRef {
        private final String id;
        private final ActorOptions options;
        private final ArrayDeque<Job<?>> queue = new ArrayDeque<>();
        private final Set<CompletableFuture<Void>> reservations = new HashSet<>();
        private final CompletableFuture<Void> started = new CompletableFuture<>(), stopped = new CompletableFuture<>();
        private boolean scheduled, stopping, initialized, finished;
        private long accepted, rejected, processed, failures, totalNanos;
        private Throwable lastFailure;
        private ActorRef(String id, ActorOptions options) { this.id = id; this.options = options; }
        public String id() { return id; }
        public void requireCurrent() {
            if (CURRENT.get() != this) throw new IllegalStateException("state access outside actor " + id);
        }
        public CompletionStage<Void> started() { return started.minimalCompletionStage(); }
        public CompletionStage<Void> stopped() { return stopped.minimalCompletionStage(); }
        public synchronized int pending() { return queue.size() + reservations.size(); }
        public synchronized ActorStats stats() {
            return new ActorStats(id, pending(), capacity, scheduled, stopping, accepted, rejected,
                processed, failures, totalNanos, lastFailure);
        }
        /** Drain accepted commands; outstanding external completions are cancelled. */
        public CompletionStage<Void> stop() {
            List<CompletableFuture<Void>> cancelled;
            synchronized (this) {
                if (stopping) return stopped();
                stopping = true;
                cancelled = List.copyOf(reservations); reservations.clear();
                schedule();
            }
            var error = new RejectedExecutionException("actor stopping: " + id);
            cancelled.forEach(f -> f.completeExceptionally(error));
            return stopped();
        }
        public <T> CompletionStage<T> call(Callable<T> action) {
            var job = new Job<T>(Objects.requireNonNull(action), new CompletableFuture<>());
            synchronized (this) {
                if (stopping || pending() >= capacity) {
                    rejected++;
                    return CompletableFuture.failedFuture(new RejectedExecutionException("mailbox unavailable: " + id));
                }
                queue.add(job); accepted++; schedule();
            }
            return job.result.minimalCompletionStage();
        }
        public CompletionStage<Void> tell(Runnable action) {
            Objects.requireNonNull(action);
            return call(() -> { action.run(); return null; });
        }
        public <T> CompletionStage<Void> pipe(Supplier<? extends CompletionStage<T>> start, BiConsumer<T, Throwable> handler) {
            requireCurrent(); Objects.requireNonNull(start); Objects.requireNonNull(handler);
            var delivered = new CompletableFuture<Void>();
            synchronized (this) {
                if (stopping || pending() >= capacity) {
                    rejected++;
                    return CompletableFuture.failedFuture(new RejectedExecutionException("callback capacity"));
                }
                reservations.add(delivered); accepted++;
            }
            try {
                Objects.requireNonNull(start.get()).whenComplete((value, error) -> deliver(delivered, () -> {
                    handler.accept(value, error); return null;
                }));
            } catch (Throwable error) {
                deliver(delivered, () -> { handler.accept(null, error); return null; });
            }
            return delivered.minimalCompletionStage();
        }
        private void deliver(CompletableFuture<Void> delivered, Callable<Void> action) {
            synchronized (this) {
                if (!reservations.remove(delivered)) return;
                queue.add(new Job<>(action, delivered)); schedule();
            }
        }
        // Called under the actor lock. Executor stays live until all actors have stopped.
        private void schedule() {
            if (scheduled || finished) return;
            scheduled = true;
            workers.execute(this::runBatch);
        }
        private void runBatch() {
            boolean resubmit = false;
            CURRENT.set(this);
            try {
                if (!initialized) {
                    initialized = true;
                    try {
                        if (options.lifecycle() != null) options.lifecycle().onStart(this);
                        started.complete(null);
                    } catch (Throwable error) {
                        handleFailure(error, true); started.completeExceptionally(error);
                    }
                }
                for (int i = 0; i < quantum; i++) {
                    Job<?> job;
                    synchronized (this) { job = queue.poll(); }
                    if (job == null) break;
                    execute(job);
                }
                boolean finish;
                synchronized (this) {
                    finish = stopping && queue.isEmpty();
                    if (finish) finished = true;
                    // Keep ownership of this actor until onStop and all hooks return.
                    else if (queue.isEmpty()) scheduled = false;
                    else resubmit = true;
                }
                if (finish) {
                    Throwable error = null;
                    try { if (options.lifecycle() != null) options.lifecycle().onStop(this); }
                    catch (Throwable e) { error = e; recordFailure(e); }
                    removeActor(this);
                    synchronized (this) { scheduled = false; }
                    if (error == null) stopped.complete(null); else stopped.completeExceptionally(error);
                }
            } finally {
                CURRENT.remove();
            }
            synchronized (this) {
                // Re-submit only when this batch retained ownership and work remains.
                if (resubmit) workers.execute(this::runBatch);
            }
        }
        private <T> void execute(Job<T> job) {
            long begin = System.nanoTime();
            T value = null; Throwable error = null;
            try { value = job.action.call(); } catch (Throwable e) { error = e; }
            synchronized (this) { totalNanos += Math.max(0, System.nanoTime() - begin); processed++; }
            if (error != null) handleFailure(error, false);
            if (error == null) job.result.complete(value); else job.result.completeExceptionally(error);
        }
        private synchronized void recordFailure(Throwable error) { failures++; lastFailure = error; }
        private void handleFailure(Throwable error, boolean fatal) {
            recordFailure(error);
            try { if (options.lifecycle() != null) options.lifecycle().onFailure(this, error); }
            catch (Throwable hookError) { recordFailure(hookError); fatal = true; }
            if (!fatal && options.failurePolicy() != FailurePolicy.STOP) return;
            List<CompletableFuture<?>> cancelled = new ArrayList<>();
            synchronized (this) {
                stopping = true;
                while (!queue.isEmpty()) { cancelled.add(queue.remove().result); rejected++; }
                cancelled.addAll(reservations); reservations.clear();
            }
            var rejection = new RejectedExecutionException("actor stopped after failure: " + id, error);
            cancelled.forEach(f -> f.completeExceptionally(rejection));
        }
    }
    private record Job<T>(Callable<T> action, CompletableFuture<T> result) {}

    @Override public void close() {
        if (CURRENT.get() != null) throw new IllegalStateException("close from actor would deadlock");
        List<ActorRef> refs;
        synchronized (this) { closed = true; refs = List.copyOf(actors.values()); }
        refs.forEach(ActorRef::stop);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try {
            for (var ref : refs) {
                try { ref.stopped.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
                catch (ExecutionException ignored) { /* onStop failure is exposed by stopped() and stats(). */ }
            }
            workers.shutdown();
            if (!workers.awaitTermination(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new IllegalStateException("workers still running");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("actor shutdown timed out", e);
        } finally {
            if (!workers.isTerminated()) workers.shutdownNow();
        }
    }
}
