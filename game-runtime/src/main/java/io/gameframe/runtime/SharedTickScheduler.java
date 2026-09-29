package io.gameframe.runtime;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Bounded shared timer. Callbacks enqueue work; they must not block or mutate actor state. */
public final class SharedTickScheduler implements AutoCloseable {
    public record Stats(int activeTasks, int maxTasks, long firedTasks, long rejectedRegistrations,
                        long failedTasks, Throwable lastFailure, boolean closed) {}
    public final class Handle implements AutoCloseable {
        private final Entry entry;
        private Handle(Entry entry) { this.entry = entry; }
        public boolean isCancelled() { synchronized (lock) { return entry.cancelled; } }
        @Override public void close() { synchronized (lock) { cancel(entry); } }
    }
    private static final class Global {
        private static final SharedTickScheduler INSTANCE = new SharedTickScheduler(
            Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() / 2)), 10_000, "game-tick");
    }
    /** Process lifetime convenience instance; applications should prefer an explicitly owned scheduler. */
    public static SharedTickScheduler global() { return Global.INSTANCE; }
    private final Object lock = new Object();
    private final ScheduledThreadPoolExecutor timer;
    private final Set<Entry> entries = new HashSet<>();
    private final int maxTasks;
    private boolean closed;
    private long fired, rejected, failed;
    private Throwable lastFailure;
    public SharedTickScheduler(int threads, int maxTasks, String name) {
        if (threads < 1 || maxTasks < 1 || name == null || name.isBlank()) throw new IllegalArgumentException();
        this.maxTasks = maxTasks;
        timer = new ScheduledThreadPoolExecutor(threads, r -> {
            var thread = new Thread(r, name); thread.setDaemon(true); return thread;
        });
        timer.setRemoveOnCancelPolicy(true);
        timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        timer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }
    public Handle schedule(Duration interval, Runnable task) {
        long nanos = Objects.requireNonNull(interval).toNanos();
        Objects.requireNonNull(task);
        if (nanos < 1_000_000) throw new IllegalArgumentException("interval must be at least 1ms");
        synchronized (lock) {
            if (closed || entries.size() >= maxTasks) {
                rejected++; throw new RejectedExecutionException("tick scheduler unavailable");
            }
            var entry = new Entry(task);
            entries.add(entry);
            try {
                // run() takes the same lock before examining the published future.
                entry.future = timer.scheduleAtFixedRate(() -> run(entry), nanos, nanos, TimeUnit.NANOSECONDS);
            } catch (RuntimeException error) { cancel(entry); rejected++; throw error; }
            return new Handle(entry);
        }
    }
    private void run(Entry entry) {
        synchronized (lock) { if (entry.cancelled) return; fired++; }
        try { entry.task.run(); }
        catch (Throwable error) {
            synchronized (lock) { failed++; lastFailure = error; cancel(entry); }
        }
    }
    private void cancel(Entry entry) {
        if (entry.cancelled) return;
        entry.cancelled = true; entries.remove(entry);
        if (entry.future != null) entry.future.cancel(false);
    }
    public Stats stats() {
        synchronized (lock) { return new Stats(entries.size(), maxTasks, fired, rejected, failed, lastFailure, closed); }
    }
    /** Cancels registrations. An already running callback may finish; no callback is interrupted. */
    @Override public void close() {
        synchronized (lock) {
            if (closed) return;
            closed = true;
            for (var entry : List.copyOf(entries)) cancel(entry);
            timer.shutdown();
        }
    }
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        return timer.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
    private static final class Entry {
        final Runnable task; boolean cancelled; ScheduledFuture<?> future;
        Entry(Runnable task) { this.task = task; }
    }
}
