package io.gameframe.storage.redis;

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

/**
 * Bounded polling watcher for Redis service leases.
 *
 * <p>Redis keyspace notifications are optional and frequently disabled in hosted
 * Redis. This watcher therefore uses bounded SCAN snapshots. It invokes the
 * listener on every successful poll so unchanged leases refresh their read-side expiry;
 * the returned boolean still reports membership or metadata changes. The callback should hand
 * work to the application's control-plane executor quickly.</p>
 */
public final class RedisServiceDirectoryWatcher implements AutoCloseable {
    private final RedisServiceLeaseRegistry registry;
    private final int maxEntries;
    private final Duration interval;
    private final Consumer<List<RedisServiceLeaseRegistry.PublishedLease>> listener;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<String> lastFingerprint = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RedisServiceDirectoryWatcher(RedisServiceLeaseRegistry registry,
                                        int maxEntries,
                                        Duration interval,
                                        Consumer<List<RedisServiceLeaseRegistry.PublishedLease>> listener) {
        this.registry = Objects.requireNonNull(registry, "registry");
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
        if (interval == null || interval.toMillis() < 10) {
            throw new IllegalArgumentException("interval must be at least 10ms");
        }
        this.maxEntries = maxEntries;
        this.interval = interval;
        this.listener = Objects.requireNonNull(listener, "listener");
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "redis-service-directory-watcher");
            thread.setDaemon(true);
            return thread;
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(factory);
    }

    /** Starts periodic snapshots and performs the first snapshot immediately. */
    public CompletionStage<Boolean> start() {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("watcher closed"));
        CompletionStage<Boolean> first = pollNow();
        scheduler.scheduleWithFixedDelay(this::pollSafely, interval.toMillis(),
                interval.toMillis(), TimeUnit.MILLISECONDS);
        return first;
    }

    /** Performs one bounded snapshot. The result says whether the listener ran. */
    public CompletionStage<Boolean> pollNow() {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("watcher closed"));
        return registry.list(maxEntries).thenApply(snapshot -> {
            if (closed.get()) return false;
            List<RedisServiceLeaseRegistry.PublishedLease> ordered = snapshot.stream()
                    .sorted(Comparator.comparing((RedisServiceLeaseRegistry.PublishedLease value) ->
                            value.registration().role())
                            .thenComparing(value -> value.registration().instanceId()))
                    .toList();
            String fingerprint = fingerprint(ordered);
            String previous = lastFingerprint.getAndSet(fingerprint);
            listener.accept(ordered);
            return !Objects.equals(previous, fingerprint);
        });
    }

    private void pollSafely() {
        try {
            pollNow().toCompletableFuture().exceptionally(error -> null);
        } catch (RuntimeException ignored) {
            // A transient Redis failure leaves the previous snapshot in place.
        }
    }

    private static String fingerprint(List<RedisServiceLeaseRegistry.PublishedLease> snapshot) {
        var builder = new StringBuilder();
        for (var published : snapshot) {
            var registration = published.registration();
            var lease = published.lease();
            builder.append(registration.role()).append('\n')
                    .append(registration.instanceId()).append('\n')
                    .append(registration.generation()).append('\n')
                    .append(lease.token()).append('\n')
                    .append(registration.endpoint()).append('\n')
                    .append(registration.weight()).append('\n')
                    .append(registration.leaseTtlMillis()).append('\n')
                    .append(registration.capabilities().stream().sorted().toList()).append('\n')
                    .append(published.load()).append('\n')
                    .append(published.draining()).append('\n');
        }
        return Integer.toHexString(builder.toString().hashCode()) + ":" + snapshot.size();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) scheduler.shutdownNow();
    }
}
