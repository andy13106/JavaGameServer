package io.gameframe.storage;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Executes asynchronous operations with explicit retry permission.
 * Attempt timeout stops waiting only; it cannot undo a side effect already accepted by a driver.
 */
public final class ResilientExecutor implements AutoCloseable {
    public enum RetryPermission { NEVER, IDEMPOTENT }
    public enum CircuitState { CLOSED, OPEN, HALF_OPEN }
    public record Stats(long calls, long attempts, long retries, long timeouts, long successes,
                        long failures, long circuitRejections, int circuits, int pendingTimers, boolean closed) {}
    public static final class AttemptTimeoutException extends TimeoutException {
        public AttemptTimeoutException(Duration timeout) { super("operation attempt exceeded " + timeout); }
    }
    public static final class CircuitOpenException extends RejectedExecutionException {
        public CircuitOpenException(String circuit) { super("circuit is open: " + circuit); }
    }

    private static final class Breaker {
        CircuitState state = CircuitState.CLOSED;
        int failures;
        long openUntil;
        boolean acquire(long now) {
            if (state == CircuitState.OPEN) {
                if (now - openUntil < 0) return false;
                state = CircuitState.HALF_OPEN;
                return true;
            }
            return state != CircuitState.HALF_OPEN;
        }
        void success() { state = CircuitState.CLOSED; failures = 0; openUntil = 0; }
        void failure(long now, ResiliencePolicy policy) {
            if (state == CircuitState.HALF_OPEN || ++failures >= policy.circuitFailureThreshold()) {
                state = CircuitState.OPEN;
                openUntil = now + policy.circuitOpenDuration().toNanos();
            }
        }
        CircuitState state(long now) {
            return state == CircuitState.OPEN && now - openUntil >= 0 ? CircuitState.HALF_OPEN : state;
        }
    }

    private final ResiliencePolicy policy;
    private final ScheduledThreadPoolExecutor timer;
    private final int maxPendingTimers;
    private final int maxCircuits;
    private final ConcurrentHashMap<String, Breaker> breakers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap.KeySetView<CompletableFuture<?>, Boolean> active = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap.KeySetView<TimerTicket, Boolean> timerTickets = ConcurrentHashMap.newKeySet();
    private final AtomicInteger pendingTimers = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final LongAdder calls = new LongAdder(), attempts = new LongAdder(), retries = new LongAdder(),
        timeouts = new LongAdder(), successes = new LongAdder(), failures = new LongAdder(),
        circuitRejections = new LongAdder();

    public ResilientExecutor(ResiliencePolicy policy, int timerThreads, int maxPendingTimers, String threadName) {
        this(policy, timerThreads, maxPendingTimers, 256, threadName);
    }

    public ResilientExecutor(ResiliencePolicy policy, int timerThreads, int maxPendingTimers,
                             int maxCircuits, String threadName) {
        this.policy = Objects.requireNonNull(policy);
        if (timerThreads < 1 || maxPendingTimers < 1 || maxCircuits < 1 ||
            threadName == null || threadName.isBlank()) throw new IllegalArgumentException();
        this.maxPendingTimers = maxPendingTimers;
        this.maxCircuits = maxCircuits;
        timer = new ScheduledThreadPoolExecutor(timerThreads, task -> {
            Thread thread = new Thread(task, threadName);
            thread.setDaemon(true);
            return thread;
        });
        timer.setRemoveOnCancelPolicy(true);
        timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public <T> CompletionStage<T> execute(String circuit, RetryPermission permission,
        Supplier<? extends CompletionStage<T>> operation, Predicate<Throwable> retryable) {
        if (circuit == null || circuit.isBlank()) throw new IllegalArgumentException("circuit required");
        Objects.requireNonNull(permission);
        Objects.requireNonNull(operation);
        Objects.requireNonNull(retryable);
        calls.increment();
        var result = new CompletableFuture<T>();
        active.add(result);
        result.whenComplete((ignored, error) -> active.remove(result));
        if (closed.get()) {
            failures.increment();
            result.completeExceptionally(new RejectedExecutionException("resilient executor closed"));
            return result.minimalCompletionStage();
        }
        var call = new Call<T>(circuit, permission, operation, retryable, result);
        call.attempt();
        return result.minimalCompletionStage();
    }

    public CircuitState circuitState(String circuit) {
        Breaker breaker = breakers.get(circuit);
        if (breaker == null) return CircuitState.CLOSED;
        synchronized (breaker) { return breaker.state(System.nanoTime()); }
    }

    public Stats stats() {
        return new Stats(calls.sum(), attempts.sum(), retries.sum(), timeouts.sum(), successes.sum(),
            failures.sum(), circuitRejections.sum(), breakers.size(), pendingTimers.get(), closed.get());
    }

    private final class Call<T> {
        private final String circuit;
        private final RetryPermission permission;
        private final Supplier<? extends CompletionStage<T>> operation;
        private final Predicate<Throwable> retryable;
        private final CompletableFuture<T> result;
        private int attempt;

        Call(String circuit, RetryPermission permission, Supplier<? extends CompletionStage<T>> operation,
             Predicate<Throwable> retryable, CompletableFuture<T> result) {
            this.circuit = circuit;
            this.permission = permission;
            this.operation = operation;
            this.retryable = retryable;
            this.result = result;
        }

        void attempt() {
            if (result.isDone()) return;
            Breaker breaker;
            try { breaker = breaker(circuit); }
            catch (Throwable error) { finishFailure(error); return; }
            synchronized (breaker) {
                if (!breaker.acquire(System.nanoTime())) {
                    circuitRejections.increment();
                    finishFailure(new CircuitOpenException(circuit));
                    return;
                }
            }
            attempt++;
            attempts.increment();
            var settled = new AtomicBoolean();
            TimerTicket timeout;
            try {
                timeout = schedule(() -> {
                    if (!settled.compareAndSet(false, true)) return;
                    timeouts.increment();
                    onAttemptFailure(breaker, new AttemptTimeoutException(policy.attemptTimeout()));
                }, policy.attemptTimeout().toNanos());
            } catch (Throwable error) {
                synchronized (breaker) {
                    if (breaker.state == CircuitState.HALF_OPEN)
                        breaker.failure(System.nanoTime(), policy);
                }
                finishFailure(unwrap(error));
                return;
            }
            CompletionStage<T> stage;
            try {
                stage = Objects.requireNonNull(operation.get(), "operation returned null stage");
            } catch (Throwable error) {
                if (settled.compareAndSet(false, true)) {
                    timeout.cancel();
                    onAttemptFailure(breaker, unwrap(error));
                }
                return;
            }
            stage.whenComplete((value, error) -> {
                if (!settled.compareAndSet(false, true)) return;
                timeout.cancel();
                if (error == null) {
                    synchronized (breaker) { breaker.success(); }
                    successes.increment();
                    result.complete(value);
                } else {
                    onAttemptFailure(breaker, unwrap(error));
                }
            });
        }

        void onAttemptFailure(Breaker breaker, Throwable error) {
            if (result.isDone()) return;
            boolean allowed;
            try { allowed = retryable.test(error); }
            catch (Throwable classifierFailure) {
                synchronized (breaker) {
                    if (breaker.state == CircuitState.HALF_OPEN)
                        breaker.failure(System.nanoTime(), policy);
                }
                finishFailure(classifierFailure);
                return;
            }
            synchronized (breaker) {
                if (allowed) breaker.failure(System.nanoTime(), policy);
                else if (breaker.state == CircuitState.HALF_OPEN) breaker.success();
            }
            if (allowed && permission == RetryPermission.IDEMPOTENT && attempt < policy.maxAttempts()) {
                retries.increment();
                try { schedule(this::attempt, policy.backoffNanos(attempt)); }
                catch (Throwable schedulingFailure) { finishFailure(unwrap(schedulingFailure)); }
            } else {
                finishFailure(error);
            }
        }

        void finishFailure(Throwable error) {
            if (result.completeExceptionally(error)) failures.increment();
        }
    }

    private Breaker breaker(String circuit) {
        Breaker existing = breakers.get(circuit);
        if (existing != null) return existing;
        synchronized (breakers) {
            existing = breakers.get(circuit);
            if (existing != null) return existing;
            if (breakers.size() >= maxCircuits)
                throw new RejectedExecutionException("resilience circuit capacity exceeded");
            Breaker created = new Breaker();
            breakers.put(circuit, created);
            return created;
        }
    }
    private TimerTicket schedule(Runnable action, long delayNanos) {
        if (closed.get()) throw new RejectedExecutionException("resilient executor closed");
        int pending = pendingTimers.incrementAndGet();
        if (pending > maxPendingTimers) {
            pendingTimers.decrementAndGet();
            throw new RejectedExecutionException("resilience timer capacity exceeded");
        }
        var ticket = new TimerTicket();
        timerTickets.add(ticket);
        try {
            ticket.setFuture(timer.schedule(() -> ticket.fire(action), delayNanos, TimeUnit.NANOSECONDS));
            return ticket;
        } catch (RuntimeException error) {
            ticket.cancel();
            throw error;
        }
    }

    private final class TimerTicket {
        private final AtomicBoolean released = new AtomicBoolean();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private volatile ScheduledFuture<?> future;

        void setFuture(ScheduledFuture<?> future) {
            this.future = future;
            if (cancelled.get()) future.cancel(false);
        }
        void fire(Runnable action) {
            if (cancelled.get()) { release(); return; }
            release();
            action.run();
        }
        void cancel() {
            cancelled.set(true);
            ScheduledFuture<?> current = future;
            if (current != null) current.cancel(false);
            release();
        }
        private void release() {
            if (released.compareAndSet(false, true)) {
                pendingTimers.decrementAndGet();
                timerTickets.remove(this);
            }
        }
    }
    private static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException) &&
               error.getCause() != null) error = error.getCause();
        return error;
    }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        var error = new RejectedExecutionException("resilient executor closed");
        for (var result : active) {
            if (result.completeExceptionally(error)) failures.increment();
        }
        for (var ticket : timerTickets) ticket.cancel();
        timer.shutdownNow();
    }
}