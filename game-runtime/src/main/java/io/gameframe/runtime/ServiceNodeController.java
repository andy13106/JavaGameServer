package io.gameframe.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates one process incarnation with the local directory, a shared
 * lease store and graceful draining. Storage modules implement {@link LeaseStore}
 * with Redis, MySQL or another control-plane backend.
 */
public final class ServiceNodeController implements AutoCloseable {
    public enum State { NEW, STARTING, ACTIVE, DRAINING, FAILED, STOPPED }

    public interface LeaseStore {
        CompletionStage<Boolean> acquire(ServiceDirectory.Registration registration,
                                         ServiceDirectory.Lease lease);
        CompletionStage<Boolean> renew(ServiceDirectory.Registration registration,
                                       ServiceDirectory.Lease lease,
                                       long nowMillis);
        /** Publishes the latest heartbeat and cross-process load/draining metadata. */
        default CompletionStage<Boolean> update(ServiceDirectory.Registration registration,
                                                ServiceDirectory.Lease lease,
                                                long nowMillis, int load, boolean draining) {
            return renew(registration, lease, nowMillis);
        }
        CompletionStage<Boolean> release(ServiceDirectory.Lease lease);
    }

    public record Config(Duration heartbeatInterval, Duration shutdownTimeout) {
        public Config {
            Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
            Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
            if (heartbeatInterval.toMillis() < 10) throw new IllegalArgumentException("heartbeatInterval must be at least 10ms");
            if (shutdownTimeout.isNegative()) throw new IllegalArgumentException("shutdownTimeout must not be negative");
        }

        public static Config defaults() { return new Config(Duration.ofSeconds(1), Duration.ofSeconds(30)); }
    }

    private final ServiceDirectory directory;
    private final GracefulDrain drain;
    private final LeaseStore store;
    private final Config config;
    private final RuntimeEventLogger eventLogger;
    private volatile ScheduledExecutorService scheduler;
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    private final AtomicInteger load = new AtomicInteger();
    private final AtomicBoolean draining = new AtomicBoolean();
    private volatile ServiceDirectory.Registration registration;
    private volatile ServiceDirectory.Lease lease;

    public ServiceNodeController(ServiceDirectory directory, GracefulDrain drain,
                                 LeaseStore store, Config config) {
        this(directory, drain, store, config, RuntimeEventLogger.noop());
    }

    public ServiceNodeController(ServiceDirectory directory, GracefulDrain drain,
                                 LeaseStore store, Config config, RuntimeEventLogger eventLogger) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.drain = Objects.requireNonNull(drain, "drain");
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        this.eventLogger = Objects.requireNonNull(eventLogger, "eventLogger");
        this.scheduler = newScheduler();
    }

    /** Registers locally, acquires the shared lease and starts renewal. */
    public CompletionStage<Boolean> start(ServiceDirectory.Registration registration, long nowMillis) {
        Objects.requireNonNull(registration, "registration");
        if (!state.compareAndSet(State.NEW, State.STARTING)) {
            return CompletableFuture.failedFuture(new IllegalStateException("node already started"));
        }
        this.registration = registration;
        load.set(0);
        draining.set(false);
        var local = directory.registerLease(registration, nowMillis);
        if (local.status() != ServiceDirectory.UpdateResult.REGISTERED) {
            state.set(State.FAILED);
            return CompletableFuture.completedFuture(false);
        }
        this.lease = local.lease();
        return store.acquire(registration, lease).handle((acquired, error) -> {
            if (error != null || !Boolean.TRUE.equals(acquired)) {
                state.set(State.FAILED);
                directory.unregister(lease, System.currentTimeMillis());
                throw new CompletionException(error == null
                        ? new IllegalStateException("shared lease rejected") : error);
            }
            state.set(State.ACTIVE);
            event("service.node.active", fields());
            event("service.node.re_registered", fields());
            scheduler.scheduleWithFixedDelay(this::renewSafely,
                    config.heartbeatInterval().toMillis(), config.heartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
            return true;
        });
    }

    /**
     * Re-registers a node after a lost shared lease. A new generation fences
     * the failed incarnation before work is admitted again.
     */
    public CompletionStage<Boolean> reRegister(long nowMillis) {
        if (drain.state() != GracefulDrain.State.ACTIVE
                || !state.compareAndSet(State.FAILED, State.STARTING)) {
            return CompletableFuture.failedFuture(new IllegalStateException("node is not recoverable"));
        }
        var previous = registration;
        if (previous == null) {
            state.set(State.FAILED);
            return CompletableFuture.failedFuture(new IllegalStateException("node was never registered"));
        }
        var refreshed = new ServiceDirectory.Registration(previous.role(), previous.instanceId(),
                previous.endpoint(), previous.capabilities(), previous.weight(), previous.leaseTtlMillis(),
                previous.generation() + 1);
        registration = refreshed;
        draining.set(false);
        var local = directory.registerLease(refreshed, nowMillis);
        if (local.status() != ServiceDirectory.UpdateResult.REGISTERED) {
            state.set(State.FAILED);
            return CompletableFuture.completedFuture(false);
        }
        lease = local.lease();
        return store.acquire(refreshed, lease).handle((acquired, error) -> {
            if (error != null || !Boolean.TRUE.equals(acquired)) {
                state.set(State.FAILED);
                directory.unregister(lease, System.currentTimeMillis());
                throw new CompletionException(error == null
                        ? new IllegalStateException("shared lease rejected") : error);
            }
            directory.setDraining(lease, false, System.currentTimeMillis());
            scheduler = newScheduler();
            state.set(State.ACTIVE);
            event("service.node.active", fields());
            scheduler.scheduleWithFixedDelay(this::renewSafely,
                    config.heartbeatInterval().toMillis(), config.heartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
            return true;
        });
    }
    /** Performs one immediate local and shared lease renewal. */
    public CompletionStage<Boolean> renewNow(long nowMillis) {
        if (state.get() != State.ACTIVE) return CompletableFuture.completedFuture(false);
        var currentRegistration = registration;
        var currentLease = lease;
        if (currentRegistration == null || currentLease == null) return CompletableFuture.completedFuture(false);
        var local = directory.heartbeat(currentLease, nowMillis);
        if (local != ServiceDirectory.UpdateResult.REGISTERED) {
            markFailed();
            return CompletableFuture.completedFuture(false);
        }
        return store.update(currentRegistration, currentLease, nowMillis, load.get(), draining.get()).thenApply(renewed -> {
            if (!Boolean.TRUE.equals(renewed)) markFailed();
            return Boolean.TRUE.equals(renewed);
        });
    }

    public GracefulDrain.Permit acquire() {
        return state.get() == State.ACTIVE ? drain.acquire() : null;
    }

    public ServiceDirectory.UpdateResult reportLoad(int load, long nowMillis) {
        if (load < 0) throw new IllegalArgumentException("load must not be negative");
        var currentLease = requireLease();
        var result = directory.reportLoad(currentLease, load, nowMillis);
        if (result == ServiceDirectory.UpdateResult.REGISTERED && state.get() == State.ACTIVE) {
            this.load.set(load);
            var currentRegistration = registration;
            store.update(currentRegistration, currentLease, nowMillis, load, draining.get())
                    .whenComplete((updated, error) -> {
                        if (error != null || !Boolean.TRUE.equals(updated)) markFailed();
                    });
        }
        return result;
    }

    /** Marks the node draining, rejects new work, waits for permits and releases its lease. */
    public CompletionStage<GracefulDrain.Result> drain() {
        return drain(config.shutdownTimeout());
    }

    public CompletionStage<GracefulDrain.Result> drain(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (state.get() == State.NEW) {
            state.set(State.STOPPED);
            event("service.node.stopped", fields());
            return CompletableFuture.completedFuture(new GracefulDrain.Result(false, 0, java.util.List.of()));
        }
        return drain.shutdown(timeout, this::markDraining,
                java.util.List.of(this::releaseLeaseAndStop));
    }

    public State state() { return state.get(); }
        /** Latest locally reported load, also published to the shared lease. */
    public int load() { return load.get(); }
    /** Whether this node has entered graceful draining. */
    public boolean draining() { return draining.get(); }
public ServiceDirectory.Registration registration() { return registration; }
    public ServiceDirectory.Lease lease() { return lease; }

    private ScheduledExecutorService newScheduler() {
        ThreadFactory factory = runnable -> {
            var thread = new Thread(runnable, "gameframe-service-heartbeat");
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }
    private void renewSafely() {
        try {
            renewNow(System.currentTimeMillis()).toCompletableFuture().join();
        } catch (RuntimeException ignored) {
            markFailed();
        }
    }

    private void markDraining() {
        state.compareAndSet(State.ACTIVE, State.DRAINING);
        draining.set(true);
        event("service.node.draining", fields());
        var currentLease = lease;
        var currentRegistration = registration;
        long now = System.currentTimeMillis();
        if (currentLease != null) {
            directory.setDraining(currentLease, true, now);
            store.update(currentRegistration, currentLease, now, load.get(), true);
        }
    }

    private void markFailed() {
        scheduler.shutdownNow();
        State previous = state.getAndSet(State.FAILED);
        if (previous == State.ACTIVE || previous == State.STARTING) {
            event("service.node.failed", fields());
            var currentLease = lease;
            if (currentLease != null) directory.setDraining(currentLease, true, System.currentTimeMillis());
        }
    }

    private void releaseLeaseAndStop() throws Exception {
        scheduler.shutdownNow();
        var currentLease = lease;
        try {
            if (currentLease != null) store.release(currentLease).toCompletableFuture().join();
        } finally {
            if (currentLease != null) directory.unregister(currentLease, System.currentTimeMillis());
            state.set(State.STOPPED);
            event("service.node.stopped", fields());
        }
    }

    private void event(String name, Map<String, ?> fields) {
        eventLogger.info(name, fields);
    }

    private Map<String, Object> fields() {
        var current = registration;
        if (current == null) return Map.of("state", state.get().name());
        return Map.of("role", current.role(), "instance", current.instanceId(),
                "generation", current.generation(), "state", state.get().name());
    }

    private ServiceDirectory.Lease requireLease() {
        var currentLease = lease;
        if (currentLease == null) throw new IllegalStateException("node not started");
        return currentLease;
    }

    @Override public void close() {
        drain().toCompletableFuture().join();
    }
}