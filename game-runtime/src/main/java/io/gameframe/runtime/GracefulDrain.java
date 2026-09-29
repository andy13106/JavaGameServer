package io.gameframe.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Coordinates graceful server draining without blocking actor workers.
 *
 * <p>Admission permits are rejected after draining starts. Shutdown waits for
 * admitted work until the deadline, then closes resources in reverse order.</p>
 */
public final class GracefulDrain {
    public enum State { ACTIVE, DRAINING, STOPPING, TERMINATED }

    public record Result(boolean timedOut, int unfinishedAtClose, List<Throwable> closeFailures) {
        public Result {
            if (unfinishedAtClose < 0) throw new IllegalArgumentException("unfinishedAtClose must not be negative");
            closeFailures = List.copyOf(closeFailures);
        }
    }

    public final class Permit implements AutoCloseable {
        private boolean released;
        private Permit() {}
        @Override public void close() {
            synchronized (GracefulDrain.this) {
                if (released) return;
                released = true;
                inFlight--;
                GracefulDrain.this.notifyAll();
            }
        }
    }

    private State state = State.ACTIVE;
    private int inFlight;
    private CompletableFuture<Result> shutdown;

    public synchronized Permit acquire() {
        if (state != State.ACTIVE) return null;
        inFlight++;
        return new Permit();
    }

    public synchronized State state() { return state; }
    public synchronized int inFlight() { return inFlight; }

    public CompletionStage<Result> shutdown(Duration timeout, Runnable onDraining,
                                            List<? extends AutoCloseable> resources) {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(onDraining, "onDraining");
        Objects.requireNonNull(resources, "resources");
        if (timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");

        CompletableFuture<Result> result;
        synchronized (this) {
            if (shutdown != null) return shutdown;
            state = State.DRAINING;
            shutdown = new CompletableFuture<>();
            result = shutdown;
        }
        try {
            onDraining.run();
        } catch (Throwable error) {
            result.completeExceptionally(error);
            synchronized (this) {
                state = State.TERMINATED;
                notifyAll();
            }
            return result;
        }
        List<AutoCloseable> closeOrder = List.copyOf(resources);
        Thread.ofVirtual().name("gameframe-graceful-drain").start(
                () -> drainAndClose(timeout, closeOrder, result));
        return result;
    }

    private void drainAndClose(Duration timeout, List<AutoCloseable> resources,
                               CompletableFuture<Result> result) {
        long deadline = deadline(timeout);
        int unfinished;
        synchronized (this) {
            while (inFlight > 0) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) break;
                try {
                    long millis = Math.max(1, Math.min(Duration.ofNanos(remainingNanos).toMillis(), 1_000));
                    wait(millis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            unfinished = inFlight;
            state = State.STOPPING;
        }
        var failures = new ArrayList<Throwable>();
        for (int index = resources.size() - 1; index >= 0; index--) {
            try { resources.get(index).close(); }
            catch (Throwable failure) { failures.add(failure); }
        }
        synchronized (this) {
            state = State.TERMINATED;
            notifyAll();
        }
        result.complete(new Result(unfinished > 0, unfinished, failures));
    }

    private static long deadline(Duration timeout) {
        long nanos;
        try { nanos = timeout.toNanos(); }
        catch (ArithmeticException overflow) { return Long.MAX_VALUE; }
        long now = System.nanoTime();
        return Long.MAX_VALUE - now < nanos ? Long.MAX_VALUE : now + nanos;
    }
}
