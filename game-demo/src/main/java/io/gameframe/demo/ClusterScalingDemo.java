package io.gameframe.demo;

import io.gameframe.runtime.GracefulDrain;
import io.gameframe.runtime.ServiceDirectory;
import io.gameframe.runtime.ServiceDirectorySnapshotTracker;
import io.gameframe.runtime.ServiceDirectoryView;
import io.gameframe.runtime.ServiceNodeController;
import io.gameframe.transport.ServiceDirectoryRpcRouter;
import io.gameframe.runtime.RpcCallExecutor;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Deterministic Gate/Game control-plane sample: scale out, recover a lost
 * lease with a new generation, route, drain and scale in. The shared store
 * stands in for Redis or MySQL so the test remains self-contained; production
 * nodes use registry.leaseStore().
 */
public final class ClusterScalingDemo {
    public record Summary(String beforeDrain, String afterRecovery, String afterDrain,
                          int registered, int remaining, int removed,
                          boolean heartbeatFailureDetected, boolean reRegistered,
                          long recoveredGeneration) {}

    private ClusterScalingDemo() {}

    public static Summary run() throws Exception {
        var shared = new SharedLeaseStore();
        var view = new ServiceDirectoryView();
        var tracker = new ServiceDirectorySnapshotTracker();
        var localA = new ServiceDirectory();
        var localB = new ServiceDirectory();
        var a = new ServiceNodeController(localA, new GracefulDrain(), shared,
                new ServiceNodeController.Config(Duration.ofSeconds(1), Duration.ofSeconds(1)));
        var b = new ServiceNodeController(localB, new GracefulDrain(), shared,
                new ServiceNodeController.Config(Duration.ofSeconds(1), Duration.ofSeconds(1)));
        try (a; b; var router = new ServiceDirectoryRpcRouter(view,
                new RpcCallExecutor(new RpcCallExecutor.Policy(1, Duration.ofSeconds(1), Duration.ZERO,
                        Duration.ZERO, 5, Duration.ofSeconds(1), 8, 64), 1, "cluster-demo"), Duration.ofMillis(100))) {
            long now = System.currentTimeMillis();
            a.start(registration("game-a", 19001), now).toCompletableFuture().get(2, TimeUnit.SECONDS);
            b.start(registration("game-b", 19002), now).toCompletableFuture().get(2, TimeUnit.SECONDS);
            tracker.reconcile(view, shared.snapshot(now), Set.of("game"), now);
            int registered = shared.size();
            String key = keyFor(view, "game-a", now);
            String before = router.call("game", Set.of("rpc"), key, "cluster-command-1", "payload",
                    now + 2_000, RpcCallExecutor.RetryMode.NEVER, error -> false,
                    (target, request) -> CompletableFuture.completedFuture(target.instanceId()))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);

            // Simulate a lost control-plane heartbeat. A higher generation
            // replaces the old lease and fences its late heartbeats.
            shared.failNextRenew("game", "game-a");
            boolean heartbeatFailureDetected = !a.renewNow(now + 100).toCompletableFuture().get(2, TimeUnit.SECONDS);
            boolean reRegistered = a.reRegister(now + 200).toCompletableFuture().get(2, TimeUnit.SECONDS);
            tracker.reconcile(view, shared.snapshot(now + 200), Set.of("game"), now + 200);
            String afterRecovery = router.call("game", Set.of("rpc"), key, "cluster-command-recovered",
                    "payload", now + 2_200, RpcCallExecutor.RetryMode.NEVER, error -> false,
                    (target, request) -> CompletableFuture.completedFuture(target.instanceId()))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);

            a.drain(Duration.ofSeconds(1)).toCompletableFuture().get(2, TimeUnit.SECONDS);
            long afterDrain = System.currentTimeMillis();
            var reconciliation = tracker.reconcile(view, shared.snapshot(afterDrain), Set.of("game"), afterDrain);
            String after = router.call("game", Set.of("rpc"), key, "cluster-command-2", "payload",
                    afterDrain + 2_000, RpcCallExecutor.RetryMode.NEVER, error -> false,
                    (target, request) -> CompletableFuture.completedFuture(target.instanceId()))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            return new Summary(before, afterRecovery, after, registered, shared.size(),
                    reconciliation.removed(), heartbeatFailureDetected, reRegistered,
                    a.registration().generation());
        }
    }

    private static String keyFor(ServiceDirectoryView view, String expected, long now) {
        for (int i = 0; i < 10_000; i++) {
            String key = "player-" + i;
            var selected = view.choose("game", Set.of("rpc"), key, now);
            if (selected.isPresent() && selected.get().instanceId().equals(expected)) return key;
        }
        throw new IllegalStateException("could not find a stable routing key for " + expected);
    }

    private static ServiceDirectory.Registration registration(String id, int port) {
        return new ServiceDirectory.Registration("game", id, URI.create("http://127.0.0.1:" + port),
                Set.of("rpc"), 1, 60_000, 1);
    }

    private record Key(String role, String instanceId) {}
    private record Entry(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease,
                         long expiresAtMillis) {}

    private static final class SharedLeaseStore implements ServiceNodeController.LeaseStore {
        private final Map<Key, Entry> entries = new HashMap<>();
        private final Set<Key> failedRenewals = new HashSet<>();

        synchronized void failNextRenew(String role, String instanceId) {
            failedRenewals.add(new Key(role, instanceId));
        }

        @Override public synchronized java.util.concurrent.CompletionStage<Boolean> acquire(
                ServiceDirectory.Registration registration, ServiceDirectory.Lease lease) {
            var key = new Key(registration.role(), registration.instanceId());
            var current = entries.get(key);
            long now = System.currentTimeMillis();
            if (current != null && current.lease().generation() >= lease.generation()
                    && current.expiresAtMillis() > now) {
                return CompletableFuture.completedFuture(false);
            }
            // A higher generation replaces the failed incarnation atomically.
            entries.put(key, new Entry(registration, lease,
                    now + registration.leaseTtlMillis()));
            return CompletableFuture.completedFuture(true);
        }

        @Override public synchronized java.util.concurrent.CompletionStage<Boolean> renew(
                ServiceDirectory.Registration registration, ServiceDirectory.Lease lease, long nowMillis) {
            var key = new Key(registration.role(), registration.instanceId());
            if (failedRenewals.remove(key)) return CompletableFuture.completedFuture(false);
            var current = entries.get(key);
            if (current == null || current.lease().token() != lease.token()
                    || current.lease().generation() != lease.generation()
                    || current.expiresAtMillis() <= nowMillis) return CompletableFuture.completedFuture(false);
            entries.put(key, new Entry(registration, lease, nowMillis + registration.leaseTtlMillis()));
            return CompletableFuture.completedFuture(true);
        }

        @Override public synchronized java.util.concurrent.CompletionStage<Boolean> release(ServiceDirectory.Lease lease) {
            var key = new Key(lease.role(), lease.instanceId());
            var current = entries.get(key);
            boolean removed = current != null && current.lease().token() == lease.token()
                    && current.lease().generation() == lease.generation();
            if (removed) entries.remove(key);
            return CompletableFuture.completedFuture(removed);
        }

        synchronized List<ServiceDirectoryView.Observation> snapshot(long nowMillis) {
            var result = new ArrayList<ServiceDirectoryView.Observation>();
            entries.values().removeIf(entry -> entry.expiresAtMillis() <= nowMillis);
            for (var entry : entries.values()) {
                result.add(new ServiceDirectoryView.Observation(entry.registration(), entry.lease(),
                        entry.expiresAtMillis(), 0, false));
            }
            return List.copyOf(result);
        }

        synchronized int size() { return entries.size(); }
    }
}