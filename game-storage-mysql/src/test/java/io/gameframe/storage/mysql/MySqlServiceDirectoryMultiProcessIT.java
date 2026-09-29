package io.gameframe.storage.mysql;

import io.gameframe.runtime.GracefulDrain;
import io.gameframe.runtime.ServiceDirectory;
import io.gameframe.runtime.ServiceDirectorySnapshotTracker;
import io.gameframe.runtime.ServiceDirectoryView;
import io.gameframe.runtime.ServiceNodeController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Real MySQL control-plane drill with two independent client connections. */
@EnabledIfEnvironmentVariable(named = "GAME_TEST_MYSQL_URL", matches = ".+")
class MySqlServiceDirectoryMultiProcessIT {
    @Test
    void propagatesLoadDrainingAndRemovalAcrossIndependentNodes() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        try (var storeA = store(); var storeB = store(); var readerStore = store()) {
            var registryA = new MySqlServiceLeaseRegistry(storeA);
            var registryB = new MySqlServiceLeaseRegistry(storeB);
            var reader = new MySqlServiceLeaseRegistry(readerStore);
            reader.initializeSchema().toCompletableFuture().get(5, TimeUnit.SECONDS);
            var localA = new ServiceDirectory();
            var localB = new ServiceDirectory();
            var controllerA = new ServiceNodeController(localA, new GracefulDrain(), registryA.leaseStore(), controllerConfig());
            var controllerB = new ServiceNodeController(localB, new GracefulDrain(), registryB.leaseStore(), controllerConfig());
            var view = new ServiceDirectoryView();
            var tracker = new ServiceDirectorySnapshotTracker();
            var role = "game-it-" + suffix;
            var idA = "node-a-" + suffix;
            var idB = "node-b-" + suffix;
            try (var bridge = new MySqlServiceDirectoryBridge(reader, role, view, tracker, 10, Duration.ofMillis(30))) {
                long now = System.currentTimeMillis();
                assertTrue(controllerA.start(registration(role, idA, 19201), now).toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertTrue(controllerB.start(registration(role, idB, 19202), now).toCompletableFuture().get(5, TimeUnit.SECONDS));

                pollUntil(bridge, () -> view.snapshot(role, System.currentTimeMillis()).size() == 2);
                assertEquals(2, view.snapshot(role, System.currentTimeMillis()).size());

                assertEquals(ServiceDirectory.UpdateResult.REGISTERED, controllerA.reportLoad(7, System.currentTimeMillis()));
                pollUntil(bridge, () -> instance(view, role, idA).load() == 7);

                var permit = controllerA.acquire();
                assertNotNull(permit);
                var drainStage = controllerA.drain(Duration.ofSeconds(2));
                assertEquals(ServiceNodeController.State.DRAINING, controllerA.state());
                pollUntil(bridge, () -> instance(view, role, idA).draining());
                permit.close();
                assertFalse(drainStage.toCompletableFuture().get(5, TimeUnit.SECONDS).timedOut());
                pollUntil(bridge, () -> view.snapshot(role, System.currentTimeMillis()).size() == 1);
                assertEquals(idB, view.snapshot(role, System.currentTimeMillis()).getFirst().instanceId());
            } finally {
                if (controllerA.state() == ServiceNodeController.State.ACTIVE || controllerA.state() == ServiceNodeController.State.DRAINING) {
                    controllerA.drain(Duration.ofSeconds(2)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                }
                if (controllerB.state() == ServiceNodeController.State.ACTIVE || controllerB.state() == ServiceNodeController.State.DRAINING) {
                    controllerB.drain(Duration.ofSeconds(2)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    void remainsConsistentUnderConcurrentMetadataPressure() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        try (var storeA = store(); var storeB = store(); var readerStore = store()) {
            var registryA = new MySqlServiceLeaseRegistry(storeA);
            var registryB = new MySqlServiceLeaseRegistry(storeB);
            var reader = new MySqlServiceLeaseRegistry(readerStore);
            reader.initializeSchema().toCompletableFuture().get(5, TimeUnit.SECONDS);
            var controllerA = new ServiceNodeController(new ServiceDirectory(), new GracefulDrain(), registryA.leaseStore(), controllerConfig());
            var controllerB = new ServiceNodeController(new ServiceDirectory(), new GracefulDrain(), registryB.leaseStore(), controllerConfig());
            var view = new ServiceDirectoryView();
            var tracker = new ServiceDirectorySnapshotTracker();
            var role = "game-pressure-" + suffix;
            var idA = "node-a-" + suffix;
            var idB = "node-b-" + suffix;
            try (var bridge = new MySqlServiceDirectoryBridge(reader, role, view, tracker, 10, Duration.ofMillis(15))) {
                long now = System.currentTimeMillis();
                assertTrue(controllerA.start(registration(role, idA, 19211), now).toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertTrue(controllerB.start(registration(role, idB, 19212), now).toCompletableFuture().get(5, TimeUnit.SECONDS));
                pollUntil(bridge, () -> view.snapshot(role, System.currentTimeMillis()).size() == 2);

                int iterations = pressureIterations();
                var gate = new CountDownLatch(1);
                var observed = new AtomicInteger();
                try (var workers = Executors.newFixedThreadPool(3)) {
                    Future<?> updateA = workers.submit(() -> updateLoads(controllerA, 0, iterations, gate));
                    Future<?> updateB = workers.submit(() -> updateLoads(controllerB, 1_000, iterations, gate));
                    Future<?> readerTask = workers.submit(() -> {
                        gate.await();
                        for (int i = 0; i < iterations * 2; i++) {
                            bridge.pollNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
                            var snapshot = view.snapshot(role, System.currentTimeMillis());
                            if (snapshot.size() > 2) throw new AssertionError("duplicate service instances observed");
                            for (var service : snapshot) {
                                if (service.load() < 0 || service.load() >= 1_000 + iterations) {
                                    throw new AssertionError("invalid load metadata: " + service.load());
                                }
                            }
                            observed.incrementAndGet();
                            Thread.sleep(5);
                        }
                        return null;
                    });
                    gate.countDown();
                    updateA.get(15, TimeUnit.SECONDS);
                    updateB.get(15, TimeUnit.SECONDS);
                    readerTask.get(15, TimeUnit.SECONDS);
                }
                assertTrue(observed.get() > 0, "reader must observe pressure snapshots");
                controllerA.renewNow(System.currentTimeMillis()).toCompletableFuture().get(5, TimeUnit.SECONDS);
                controllerB.renewNow(System.currentTimeMillis()).toCompletableFuture().get(5, TimeUnit.SECONDS);
                int lastA = iterations - 1;
                int lastB = 1_000 + iterations - 1;
                pollUntil(bridge, () -> instance(view, role, idA).load() == lastA
                        && instance(view, role, idB).load() == lastB);
            } finally {
                if (controllerA.state() == ServiceNodeController.State.ACTIVE || controllerA.state() == ServiceNodeController.State.DRAINING) {
                    controllerA.drain(Duration.ofSeconds(2)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                }
                if (controllerB.state() == ServiceNodeController.State.ACTIVE || controllerB.state() == ServiceNodeController.State.DRAINING) {
                    controllerB.drain(Duration.ofSeconds(2)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static Void updateLoads(ServiceNodeController controller, int offset, int iterations, CountDownLatch gate)
            throws Exception {
        gate.await();
        for (int i = 0; i < iterations; i++) {
            controller.reportLoad(offset + i, System.currentTimeMillis());
        }
        return null;
    }

    private static int pressureIterations() {
        String value = System.getenv("GAME_TEST_DIRECTORY_PRESSURE_ITERATIONS");
        if (value == null || value.isBlank()) return 24;
        int parsed = Integer.parseInt(value);
        if (parsed < 8 || parsed > 200) throw new IllegalArgumentException("pressure iterations must be between 8 and 200");
        return parsed;
    }
    private static MySqlStore store() {
        return new MySqlStore(System.getenv("GAME_TEST_MYSQL_URL"), System.getenv("GAME_TEST_MYSQL_USER"),
                System.getenv("GAME_TEST_MYSQL_PASSWORD"), 2, 32);
    }

    private static ServiceNodeController.Config controllerConfig() {
        return new ServiceNodeController.Config(Duration.ofMillis(50), Duration.ofSeconds(2));
    }

    private static ServiceDirectory.Registration registration(String role, String instanceId, int port) {
        return new ServiceDirectory.Registration(role, instanceId, URI.create("http://127.0.0.1:" + port),
                Set.of("rpc", "scene"), 1, 5_000, 1);
    }

    private static ServiceDirectory.ServiceInstance instance(ServiceDirectoryView view, String role, String instanceId) {
        return view.snapshot(role, System.currentTimeMillis()).stream()
                .filter(value -> value.instanceId().equals(instanceId)).findFirst().orElseThrow();
    }

    private static void pollUntil(MySqlServiceDirectoryBridge bridge, java.util.function.BooleanSupplier condition)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            bridge.pollNow().toCompletableFuture().get(5, TimeUnit.SECONDS);
            if (condition.getAsBoolean()) return;
            Thread.sleep(30);
        }
        fail("condition did not become true before MySQL snapshot deadline");
    }
}