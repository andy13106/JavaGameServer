package io.gameframe.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Executes RPC calls with a total deadline, finite retry and per-destination circuit breaking. */
public final class RpcCallExecutor implements AutoCloseable {
    public enum RetryMode { NEVER, IDEMPOTENT }
    public enum CircuitState { CLOSED, OPEN, HALF_OPEN }

    public record Policy(int maxAttempts, Duration attemptTimeout, Duration initialBackoff,
                         Duration maxBackoff, int circuitFailureThreshold,
                         Duration circuitOpenDuration, int maxCircuits, int maxPendingTimers) {
        public Policy {
            if (maxAttempts < 1 || circuitFailureThreshold < 1 || maxCircuits < 1 || maxPendingTimers < 1)
                throw new IllegalArgumentException("RPC policy counts must be positive");
            requirePositive(attemptTimeout, "attemptTimeout");
            requireNonNegative(initialBackoff, "initialBackoff");
            requireNonNegative(maxBackoff, "maxBackoff");
            requirePositive(circuitOpenDuration, "circuitOpenDuration");
            if (initialBackoff.compareTo(maxBackoff) > 0)
                throw new IllegalArgumentException("initialBackoff exceeds maxBackoff");
        }
        public static Policy defaults() {
            return new Policy(3, Duration.ofSeconds(2), Duration.ofMillis(25), Duration.ofMillis(500),
                    5, Duration.ofSeconds(5), 256, 16_384);
        }
        long backoffMillis(int completedAttempt) {
            long value = initialBackoff.toMillis(), cap = maxBackoff.toMillis();
            for (int i = 1; i < completedAttempt && value < cap; i++) value = value > cap / 2 ? cap : value * 2;
            return Math.min(value, cap);
        }
    }

    public record Stats(long calls, long attempts, long retries, long timeouts, long successes,
                        long failures, long circuitRejections, int circuits, int pendingTimers, boolean closed) {}
    public static final class RpcDeadlineException extends TimeoutException {
        public RpcDeadlineException(String commandId) { super("RPC deadline exceeded: " + commandId); }
    }
    public static final class AttemptTimeoutException extends TimeoutException {
        public AttemptTimeoutException(String commandId) { super("RPC attempt timed out: " + commandId); }
    }
    public static final class CircuitOpenException extends RejectedExecutionException {
        public CircuitOpenException(String destination) { super("RPC circuit is open: " + destination); }
    }

    private static final class Breaker {
        CircuitState state = CircuitState.CLOSED;
        int failures;
        long openUntilMillis;
        boolean acquire(long nowMillis) {
            if (state == CircuitState.OPEN) {
                if (nowMillis < openUntilMillis) return false;
                state = CircuitState.HALF_OPEN;
                return true;
            }
            return state != CircuitState.HALF_OPEN;
        }
        void success() { state = CircuitState.CLOSED; failures = 0; openUntilMillis = 0; }
        void failure(long nowMillis, Policy policy) {
            if (state == CircuitState.HALF_OPEN || ++failures >= policy.circuitFailureThreshold()) {
                state = CircuitState.OPEN;
                openUntilMillis = safeAdd(nowMillis, policy.circuitOpenDuration().toMillis());
            }
        }
        CircuitState state(long nowMillis) {
            return state == CircuitState.OPEN && nowMillis >= openUntilMillis ? CircuitState.HALF_OPEN : state;
        }
    }

    private final Policy policy;
    private final ScheduledThreadPoolExecutor timer;
    private final ConcurrentHashMap<String, Breaker> breakers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap.KeySetView<CompletableFuture<?>, Boolean> active = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap.KeySetView<TimerTicket, Boolean> timerTickets = ConcurrentHashMap.newKeySet();
    private final AtomicInteger pendingTimers = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final LongAdder calls = new LongAdder(), attempts = new LongAdder(), retries = new LongAdder(),
            timeouts = new LongAdder(), successes = new LongAdder(), failures = new LongAdder(),
            circuitRejections = new LongAdder();

    public RpcCallExecutor() { this(Policy.defaults(), 1, "game-rpc-timer"); }
    public RpcCallExecutor(Policy policy, int timerThreads, String threadName) {
        this.policy = Objects.requireNonNull(policy, "policy");
        if (timerThreads < 1 || threadName == null || threadName.isBlank()) throw new IllegalArgumentException();
        timer = new ScheduledThreadPoolExecutor(timerThreads, task -> {
            Thread thread = new Thread(task, threadName);
            thread.setDaemon(true);
            return thread;
        });
        timer.setRemoveOnCancelPolicy(true);
        timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public <T> CompletionStage<T> execute(String destination, String commandId, RetryMode retryMode,
                                           long deadlineMillis,
                                           Supplier<? extends CompletionStage<T>> operation,
                                           Predicate<Throwable> retryable) {
        requireText(destination, "destination");
        requireText(commandId, "commandId");
        Objects.requireNonNull(retryMode, "retryMode");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(retryable, "retryable");
        if (deadlineMillis <= System.currentTimeMillis()) throw new IllegalArgumentException("deadline must be in the future");
        calls.increment();
        var result = new CompletableFuture<T>();
        active.add(result);
        result.whenComplete((ignored, error) -> active.remove(result));
        if (closed.get()) {
            finishFailure(result, new RejectedExecutionException("RPC executor closed"));
            return result.minimalCompletionStage();
        }
        new Call<>(destination, commandId, retryMode, deadlineMillis, operation, retryable, result).attempt();
        return result.minimalCompletionStage();
    }

    public CircuitState circuitState(String destination) {
        requireText(destination, "destination");
        Breaker breaker = breakers.get(destination);
        if (breaker == null) return CircuitState.CLOSED;
        synchronized (breaker) { return breaker.state(System.currentTimeMillis()); }
    }
    public Stats stats() {
        return new Stats(calls.sum(), attempts.sum(), retries.sum(), timeouts.sum(), successes.sum(),
                failures.sum(), circuitRejections.sum(), breakers.size(), pendingTimers.get(), closed.get());
    }

    private final class Call<T> {
        private final String destination, commandId;
        private final RetryMode retryMode;
        private final long deadlineMillis;
        private final Supplier<? extends CompletionStage<T>> operation;
        private final Predicate<Throwable> retryable;
        private final CompletableFuture<T> result;
        private int attempt;

        private Call(String destination, String commandId, RetryMode retryMode, long deadlineMillis,
                     Supplier<? extends CompletionStage<T>> operation, Predicate<Throwable> retryable,
                     CompletableFuture<T> result) {
            this.destination = destination;
            this.commandId = commandId;
            this.retryMode = retryMode;
            this.deadlineMillis = deadlineMillis;
            this.operation = operation;
            this.retryable = retryable;
            this.result = result;
        }

        void attempt() {
            if (result.isDone()) return;
            long now = System.currentTimeMillis(), remaining = deadlineMillis - now;
            if (remaining <= 0) { finishFailure(result, new RpcDeadlineException(commandId)); return; }
            Breaker breaker;
            try { breaker = breaker(destination); }
            catch (Throwable error) { finishFailure(result, error); return; }
            synchronized (breaker) {
                if (!breaker.acquire(now)) {
                    circuitRejections.increment();
                    finishFailure(result, new CircuitOpenException(destination));
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
                    Throwable timeoutError = System.currentTimeMillis() >= deadlineMillis
                            ? new RpcDeadlineException(commandId) : new AttemptTimeoutException(commandId);
                    onAttemptFailure(breaker, timeoutError);
                }, Math.min(remaining, policy.attemptTimeout().toMillis()));
            } catch (Throwable error) {
                synchronized (breaker) { if (breaker.state == CircuitState.HALF_OPEN) breaker.failure(now, policy); }
                finishFailure(result, unwrap(error));
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
                } else onAttemptFailure(breaker, unwrap(error));
            });
        }

        void onAttemptFailure(Breaker breaker, Throwable error) {
            if (result.isDone()) return;
            if (error instanceof RpcDeadlineException) {
                finishFailure(result, error);
                return;
            }
            boolean canRetry;
            try { canRetry = retryable.test(error); }
            catch (Throwable classifierFailure) { finishFailure(result, classifierFailure); return; }
            synchronized (breaker) {
                if (canRetry) breaker.failure(System.currentTimeMillis(), policy);
                else if (breaker.state == CircuitState.HALF_OPEN) breaker.success();
            }
            if (canRetry && retryMode == RetryMode.IDEMPOTENT && attempt < policy.maxAttempts()) {
                long delay = policy.backoffMillis(attempt);
                if (deadlineMillis - System.currentTimeMillis() <= delay) {
                    finishFailure(result, new RpcDeadlineException(commandId));
                    return;
                }
                retries.increment();
                try { schedule(this::attempt, delay); }
                catch (Throwable schedulingFailure) { finishFailure(result, unwrap(schedulingFailure)); }
            } else finishFailure(result, error);
        }
    }

    private Breaker breaker(String destination) {
        Breaker existing = breakers.get(destination);
        if (existing != null) return existing;
        synchronized (breakers) {
            existing = breakers.get(destination);
            if (existing != null) return existing;
            if (breakers.size() >= policy.maxCircuits()) throw new RejectedExecutionException("RPC circuit capacity exceeded");
            Breaker created = new Breaker();
            breakers.put(destination, created);
            return created;
        }
    }
    private TimerTicket schedule(Runnable action, long delayMillis) {
        if (closed.get()) throw new RejectedExecutionException("RPC executor closed");
        if (pendingTimers.incrementAndGet() > policy.maxPendingTimers()) {
            pendingTimers.decrementAndGet();
            throw new RejectedExecutionException("RPC timer capacity exceeded");
        }
        var ticket = new TimerTicket();
        timerTickets.add(ticket);
        try {
            ticket.setFuture(timer.schedule(() -> ticket.fire(action), Math.max(0, delayMillis), TimeUnit.MILLISECONDS));
            return ticket;
        } catch (RuntimeException error) {
            ticket.cancel();
            throw error;
        }
    }

    private final class TimerTicket {
        private final AtomicBoolean released = new AtomicBoolean(), cancelled = new AtomicBoolean();
        private volatile ScheduledFuture<?> future;
        void setFuture(ScheduledFuture<?> future) { this.future = future; if (cancelled.get()) future.cancel(false); }
        void fire(Runnable action) { if (cancelled.get()) { release(); return; } release(); action.run(); }
        void cancel() { cancelled.set(true); ScheduledFuture<?> current = future; if (current != null) current.cancel(false); release(); }
        private void release() {
            if (released.compareAndSet(false, true)) { pendingTimers.decrementAndGet(); timerTickets.remove(this); }
        }
    }

    private void finishFailure(CompletableFuture<?> result, Throwable error) {
        if (result.completeExceptionally(error)) failures.increment();
    }
    private static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null)
            error = error.getCause();
        return error;
    }
    private static long safeAdd(long value, long increment) {
        return Long.MAX_VALUE - value < increment ? Long.MAX_VALUE : value + increment;
    }
    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }
    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) throw new IllegalArgumentException(name + " must be positive");
        value.toMillis();
    }
    private static void requireNonNegative(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) throw new IllegalArgumentException(name + " must not be negative");
        value.toMillis();
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        var failure = new RejectedExecutionException("RPC executor closed");
        for (var result : active) finishFailure(result, failure);
        for (var ticket : timerTickets) ticket.cancel();
        timer.shutdownNow();
    }
}