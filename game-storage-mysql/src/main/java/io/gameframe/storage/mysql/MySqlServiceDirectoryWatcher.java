package io.gameframe.storage.mysql;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Bounded polling watcher for one MySQL service role. */
public final class MySqlServiceDirectoryWatcher implements AutoCloseable {
    private final MySqlServiceLeaseRegistry registry;
    private final String role;
    private final int maxEntries;
    private final Duration interval;
    private final Consumer<List<MySqlServiceLeaseRegistry.PublishedLease>> listener;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<String> lastFingerprint = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public MySqlServiceDirectoryWatcher(MySqlServiceLeaseRegistry registry, String role,
                                        int maxEntries, Duration interval,
                                        Consumer<List<MySqlServiceLeaseRegistry.PublishedLease>> listener) {
        this.registry = Objects.requireNonNull(registry, "registry");
        if (role == null || role.isBlank()) throw new IllegalArgumentException("role required");
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
        if (interval == null || interval.toMillis() < 10) throw new IllegalArgumentException("interval must be at least 10ms");
        this.role = role;
        this.maxEntries = maxEntries;
        this.interval = interval;
        this.listener = Objects.requireNonNull(listener, "listener");
        ThreadFactory factory = runnable -> {
            var thread = new Thread(runnable, "mysql-service-directory-watcher");
            thread.setDaemon(true);
            return thread;
        };
        scheduler = Executors.newSingleThreadScheduledExecutor(factory);
    }

    public CompletionStage<Boolean> start() {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("watcher closed"));
        var first = pollNow();
        scheduler.scheduleWithFixedDelay(this::pollSafely, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        return first;
    }

    public CompletionStage<Boolean> pollNow() {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("watcher closed"));
        return registry.list(role, maxEntries).thenApply(snapshot -> {
            if (closed.get()) return false;
            var ordered = snapshot.stream()
                    .sorted(Comparator.comparing((MySqlServiceLeaseRegistry.PublishedLease value) -> value.registration().instanceId()))
                    .toList();
            String fingerprint = fingerprint(ordered);
            String previous = lastFingerprint.getAndSet(fingerprint);
            listener.accept(ordered);
            return !Objects.equals(previous, fingerprint);
        });
    }

    private void pollSafely() {
        try { pollNow().toCompletableFuture().exceptionally(error -> null); }
        catch (RuntimeException ignored) { }
    }

    private static String fingerprint(List<MySqlServiceLeaseRegistry.PublishedLease> snapshot) {
        var builder = new StringBuilder();
        for (var published : snapshot) {
            var registration = published.registration();
            builder.append(registration.instanceId()).append('\n')
                    .append(registration.generation()).append('\n')
                    .append(published.lease().token()).append('\n')
                    .append(published.expiresAtMillis()).append('\n');
        }
        return Integer.toHexString(builder.toString().hashCode()) + ":" + snapshot.size();
    }

    @Override public void close() {
        if (closed.compareAndSet(false, true)) scheduler.shutdownNow();
    }
}