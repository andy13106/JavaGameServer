package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ServiceNodeControllerTest {
    @Test
    void startsRenewsReportsLoadAndDrainsSharedLease() throws Exception {
        var directory = new ServiceDirectory();
        var store = new FakeLeaseStore();
        var controller = new ServiceNodeController(directory, new GracefulDrain(), store,
                new ServiceNodeController.Config(Duration.ofMillis(20), Duration.ofSeconds(1)));
        long now = System.currentTimeMillis();
        var registration = registration("game-1", 60_000);
        assertTrue(controller.start(registration, now).toCompletableFuture().get());
        assertEquals(ServiceNodeController.State.ACTIVE, controller.state());
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, controller.reportLoad(7, now + 1));
        assertEquals(7, store.lastLoad.get());
        assertEquals(7, controller.load());
        assertFalse(store.lastDraining.get());
        assertTrue(controller.renewNow(now + 2).toCompletableFuture().get());
        var permit = controller.acquire();
        assertNotNull(permit);
        var resultStage = controller.drain(Duration.ofSeconds(1));
        assertEquals(ServiceNodeController.State.DRAINING, controller.state());
        assertTrue(controller.draining());
        assertNull(controller.acquire());
        permit.close();
        assertFalse(resultStage.toCompletableFuture().get().timedOut());
        assertEquals(ServiceNodeController.State.STOPPED, controller.state());
        assertEquals(1, store.releases.get());
        assertTrue(store.lastDraining.get());
        assertTrue(directory.snapshot("game", System.currentTimeMillis()).isEmpty());
    }

    @Test
    void failedNodeCanReRegisterWithNewGeneration() throws Exception {
        var directory = new ServiceDirectory();
        var store = new FakeLeaseStore();
        var controller = new ServiceNodeController(directory, new GracefulDrain(), store,
                new ServiceNodeController.Config(Duration.ofMillis(20), Duration.ofSeconds(1)));
        var registration = registration("game-reconnect", 60_000);
        assertTrue(controller.start(registration, System.currentTimeMillis()).toCompletableFuture().get());
        store.renewed.set(false);
        assertFalse(controller.renewNow(System.currentTimeMillis()).toCompletableFuture().get());
        assertEquals(ServiceNodeController.State.FAILED, controller.state());
        store.renewed.set(true);
        assertTrue(controller.reRegister(System.currentTimeMillis()).toCompletableFuture().get());
        assertEquals(ServiceNodeController.State.ACTIVE, controller.state());
        assertEquals(2, controller.registration().generation());
        assertFalse(directory.snapshot("game", System.currentTimeMillis()).getFirst().draining());
        controller.drain(Duration.ofSeconds(1)).toCompletableFuture().get();
    }

    @Test
    void emitsStructuredLifecycleEventsWithoutExposingLeaseToken() throws Exception {
        var events = new ArrayList<RuntimeEventLogger.Event>();
        var controller = new ServiceNodeController(new ServiceDirectory(), new GracefulDrain(), new FakeLeaseStore(),
                new ServiceNodeController.Config(Duration.ofMillis(20), Duration.ofSeconds(1)),
                new RuntimeEventLogger(events::add));
        assertTrue(controller.start(registration("game-logging", 60_000), System.currentTimeMillis())
                .toCompletableFuture().get());
        controller.drain(Duration.ofSeconds(1)).toCompletableFuture().get();
        assertTrue(events.stream().anyMatch(event -> event.name().equals("service.node.active")));
        assertTrue(events.stream().anyMatch(event -> event.name().equals("service.node.draining")));
        assertTrue(events.stream().anyMatch(event -> event.name().equals("service.node.stopped")));
        assertTrue(events.stream().allMatch(event -> event.fields().values().stream()
                .noneMatch(value -> value.contains("token"))));
    }

    @Test
    void rejectsSharedLeaseAndRemovesLocalRegistration() throws Exception {
        var directory = new ServiceDirectory();
        var store = new FakeLeaseStore();
        store.accept = false;
        var controller = new ServiceNodeController(directory, new GracefulDrain(), store,
                new ServiceNodeController.Config(Duration.ofMillis(20), Duration.ofSeconds(1)));
        assertThrows(Exception.class, () -> controller.start(registration("game-2", 60_000), System.currentTimeMillis())
                .toCompletableFuture().join());
        assertEquals(ServiceNodeController.State.FAILED, controller.state());
        assertTrue(directory.snapshot("game", System.currentTimeMillis()).isEmpty());
    }

    @Test
    void failedRenewalFencesNodeAndRejectsNewWork() throws Exception {
        var directory = new ServiceDirectory();
        var store = new FakeLeaseStore();
        var controller = new ServiceNodeController(directory, new GracefulDrain(), store,
                new ServiceNodeController.Config(Duration.ofMillis(20), Duration.ofSeconds(1)));
        assertTrue(controller.start(registration("game-3", 60_000), System.currentTimeMillis()).toCompletableFuture().get());
        store.renewed.set(false);
        assertFalse(controller.renewNow(System.currentTimeMillis()).toCompletableFuture().get());
        assertEquals(ServiceNodeController.State.FAILED, controller.state());
        assertNull(controller.acquire());
    }

    private static ServiceDirectory.Registration registration(String id, long ttl) {
        return new ServiceDirectory.Registration("game", id, URI.create("http://127.0.0.1:19000"),
                Set.of("rpc"), 1, ttl, 1);
    }

    private static final class FakeLeaseStore implements ServiceNodeController.LeaseStore {
        private boolean accept = true;
        private final AtomicBoolean renewed = new AtomicBoolean(true);
        private final AtomicInteger releases = new AtomicInteger();
        private final AtomicInteger lastLoad = new AtomicInteger();
        private final AtomicBoolean lastDraining = new AtomicBoolean();
        @Override public java.util.concurrent.CompletionStage<Boolean> acquire(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease) {
            return CompletableFuture.completedFuture(accept);
        }
        @Override public java.util.concurrent.CompletionStage<Boolean> renew(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease, long nowMillis) {
            return CompletableFuture.completedFuture(renewed.get());
        }
        @Override public java.util.concurrent.CompletionStage<Boolean> update(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease, long nowMillis, int load, boolean draining) {
            lastLoad.set(load);
            lastDraining.set(draining);
            return CompletableFuture.completedFuture(renewed.get());
        }
        @Override public java.util.concurrent.CompletionStage<Boolean> release(ServiceDirectory.Lease lease) {
            releases.incrementAndGet();
            return CompletableFuture.completedFuture(true);
        }
    }
}