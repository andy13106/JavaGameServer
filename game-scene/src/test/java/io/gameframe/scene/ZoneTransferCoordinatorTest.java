package io.gameframe.scene;
import io.gameframe.runtime.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class ZoneTransferCoordinatorTest {
    static final Duration TIMEOUT = Duration.ofSeconds(2);
    static <T> T get(CompletionStage<T> stage) throws Exception { return stage.toCompletableFuture().get(4, TimeUnit.SECONDS); }
    static void await(CountDownLatch latch) {
        try { assertTrue(latch.await(4, TimeUnit.SECONDS)); }
        catch (InterruptedException e) { throw new AssertionError(e); }
    }
    static class Zone implements ZoneTransferCoordinator.Zone {
        final ActorSystem.ActorRef actor;
        final Map<Long, byte[]> active = new HashMap<>(), staged = new HashMap<>();
        final Set<Long> frozen = new HashSet<>();
        final List<String> events;
        boolean accept = true, failRelease, failCommit;
        CountDownLatch preparing, releasePrepare, releasing, releaseOwnership;
        Zone(ActorSystem.ActorRef actor, List<String> events) { this.actor = actor; this.events = events; }
        void event(String value) { actor.requireCurrent(); events.add(value); }
        public void freeze(ZoneTransferCoordinator.TransferRequest r) {
            event("freeze"); assertTrue(active.containsKey(r.entityId())); assertTrue(frozen.add(r.entityId()));
        }
        public void release(ZoneTransferCoordinator.TransferRequest r) {
            event("release"); assertTrue(frozen.remove(r.entityId())); assertNotNull(active.remove(r.entityId()));
            if (releasing != null) { releasing.countDown(); await(releaseOwnership); }
            if (failRelease) throw new IllegalStateException("release after mutation");
        }
        public void resume(ZoneTransferCoordinator.TransferRequest r) { event("resume"); frozen.remove(r.entityId()); }
        public boolean prepare(ZoneTransferCoordinator.TransferRequest r) {
            event("prepare"); staged.put(r.entityId(), r.payload());
            if (preparing != null) { preparing.countDown(); await(releasePrepare); }
            return accept;
        }
        public void commit(ZoneTransferCoordinator.TransferRequest r) {
            event("commit"); assertNull(active.put(r.entityId(), staged.remove(r.entityId())));
            if (failCommit) throw new IllegalStateException("commit after mutation");
        }
        public void cancel(ZoneTransferCoordinator.TransferRequest r) { event("cancel"); staged.remove(r.entityId()); }
    }
    static final class Fixture implements AutoCloseable {
        final ActorSystem actors;
        final SharedTickScheduler timer = new SharedTickScheduler(1, 32, "zone-test");
        final ZoneTransferCoordinator coordinator;
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final Zone source, target;
        Fixture(int capacity, int maxPending) throws Exception {
            actors = new ActorSystem(2, 2, capacity, 1);
            coordinator = new ZoneTransferCoordinator(timer, maxPending);
            source = new Zone(actors.spawn("source"), events); target = new Zone(actors.spawn("target"), events);
            coordinator.registerZone("source", source.actor, source); coordinator.registerZone("target", target.actor, target);
            get(source.actor.tell(() -> { source.active.put(7L, new byte[]{1,2}); source.active.put(8L, new byte[]{3}); }));
        }
        ZoneTransferCoordinator.Transfer begin(long id, Duration timeout) throws Exception {
            return get(source.actor.call(() -> coordinator.begin("source", "target", id, source.active.get(id), timeout)));
        }
        void blockPrepare() { target.preparing = new CountDownLatch(1); target.releasePrepare = new CountDownLatch(1); }
        public void close() {
            if (target.releasePrepare != null) target.releasePrepare.countDown();
            if (source.releaseOwnership != null) source.releaseOwnership.countDown();
            coordinator.close(); actors.close(); timer.close();
        }
    }
    @Test void successfulHandoffHasSingleOwnerAndCopiesPayload() throws Exception {
        try (var f = new Fixture(16, 8)) {
            var t = f.begin(7, TIMEOUT);
            byte[] copy = t.request().payload(); copy[0] = 99;
            assertTrue(get(t.result()).accepted());
            assertEquals(List.of("freeze", "prepare", "release", "commit"), f.events);
            assertFalse(get(f.source.actor.call(() -> f.source.active.containsKey(7L))));
            assertArrayEquals(new byte[]{1,2}, get(f.target.actor.call(() -> f.target.active.get(7L))));
            t.cancel(); t.cancel();
            assertFalse(get(f.target.actor.call(() -> f.coordinator.confirmPrepared(t))));
            assertFalse(get(f.target.actor.call(() -> f.coordinator.confirmPrepared(t))));
            assertEquals(0, f.coordinator.pendingCount()); assertEquals(0, f.timer.stats().activeTasks());
        }
    }
    @Test void targetRejectionCancelsStagingBeforeSourceResumes() throws Exception {
        try (var f = new Fixture(16, 8)) {
            f.target.accept = false;
            var t = f.begin(7, TIMEOUT);
            assertEquals(ZoneTransferCoordinator.Outcome.REJECTED, get(t.result()).outcome());
            assertEquals(List.of("freeze", "prepare", "cancel", "resume"), f.events);
            assertTrue(get(f.source.actor.call(() -> f.source.active.containsKey(7L) && f.source.frozen.isEmpty())));
            assertTrue(get(f.target.actor.call(() -> f.target.staged.isEmpty() && f.target.active.isEmpty())));
        }
    }
    @Test void timeoutAndLatePrepareNeverReleaseOrActivateAndDuplicateCancelIsHarmless() throws Exception {
        try (var f = new Fixture(16, 1)) {
            f.blockPrepare();
            var t = f.begin(7, Duration.ofMillis(100));
            await(f.target.preparing);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (t.phase() != ZoneTransferCoordinator.Phase.ABORTING && System.nanoTime() < deadline) Thread.onSpinWait();
            assertEquals(ZoneTransferCoordinator.Phase.ABORTING, t.phase());
            t.cancel(); t.cancel();
            assertEquals(1, f.coordinator.pendingCount()); // rollback has not been confirmed
            f.target.releasePrepare.countDown();
            assertEquals(ZoneTransferCoordinator.Outcome.TIMEOUT, get(t.result()).outcome());
            assertFalse(get(f.target.actor.call(() -> f.coordinator.confirmPrepared(t))));
            assertEquals(List.of("freeze", "prepare", "cancel", "resume"), f.events);
            assertEquals(0, f.coordinator.pendingCount());
        }
    }
    @Test void pendingAndDuplicateEntityAdmissionAreBoundedBeforeFreeze() throws Exception {
        try (var f = new Fixture(16, 1)) {
            f.blockPrepare(); var t = f.begin(7, TIMEOUT); await(f.target.preparing);
            assertThrows(ExecutionException.class, () -> f.begin(8, TIMEOUT));
            assertThrows(ExecutionException.class, () -> f.begin(7, TIMEOUT));
            assertEquals(1, f.coordinator.pendingCount());
            f.target.releasePrepare.countDown(); assertTrue(get(t.result()).accepted());
            assertEquals(1, Collections.frequency(f.events, "freeze"));
        }
    }
    @Test void failedReleaseRetainsRecoverySlotAndNeverActivatesTargetOrResumesSource() throws Exception {
        try (var f = new Fixture(16, 1)) {
            f.source.failRelease = true;
            var t = f.begin(7, TIMEOUT);
            assertEquals(ZoneTransferCoordinator.Outcome.RECOVERY_REQUIRED, get(t.result()).outcome());
            get(f.source.actor.tell(() -> {})); get(f.target.actor.tell(() -> {}));
            assertEquals(List.of("freeze", "prepare", "release"), f.events);
            assertEquals(1, f.coordinator.pendingCount());
            assertFalse(get(f.source.actor.call(() -> f.source.active.containsKey(7L))));
            assertTrue(get(f.target.actor.call(() -> f.target.active.isEmpty())));
            // Application reconciles by discarding staging; no actor owns the entity after this choice.
            get(f.target.actor.tell(() -> f.target.cancel(t.request())));
            f.coordinator.acknowledgeRecovery(t);
            assertEquals(0, f.coordinator.pendingCount());
        }
    }
    @Test void failedCommitDoesNotReactivateSource() throws Exception {
        try (var f = new Fixture(16, 8)) {
            f.target.failCommit = true;
            var t = f.begin(7, TIMEOUT);
            assertEquals(ZoneTransferCoordinator.Outcome.RECOVERY_REQUIRED, get(t.result()).outcome());
            assertFalse(get(f.source.actor.call(() -> f.source.active.containsKey(7L))));
            assertTrue(get(f.target.actor.call(() -> f.target.active.containsKey(7L))));
            assertFalse(f.events.contains("resume"));
        }
    }
    @Test void targetMailboxRejectionIsReportedAndRecoveryRemainsBounded() throws Exception {
        try (var f = new Fixture(1, 1)) {
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var running = f.target.actor.tell(() -> { entered.countDown(); await(release); });
            await(entered);
            var queued = f.target.actor.tell(() -> {});
            try {
                var t = f.begin(7, TIMEOUT);
                assertEquals(ZoneTransferCoordinator.Outcome.RECOVERY_REQUIRED, get(t.result()).outcome());
                assertEquals(1, f.coordinator.pendingCount());
                assertEquals(List.of("freeze"), f.events);
            } finally { release.countDown(); get(running); get(queued); }
        }
    }
    @Test void sourceStopDuringPreparePreventsCommitAndExposesUnconfirmedResume() throws Exception {
        try (var f = new Fixture(16, 1)) {
            f.blockPrepare(); var t = f.begin(7, TIMEOUT); await(f.target.preparing);
            get(f.source.actor.stop()); f.target.releasePrepare.countDown();
            assertEquals(ZoneTransferCoordinator.Outcome.RECOVERY_REQUIRED, get(t.result()).outcome());
            assertTrue(get(f.target.actor.call(() -> f.target.active.isEmpty())));
            assertFalse(f.events.contains("commit")); assertEquals(1, f.coordinator.pendingCount());
        }
    }
    @Test void coordinatorCloseCancelsBeforeReleaseAndRejectsNewAdmission() throws Exception {
        try (var f = new Fixture(16, 1)) {
            f.blockPrepare(); var t = f.begin(7, TIMEOUT); await(f.target.preparing);
            f.coordinator.close(); f.target.releasePrepare.countDown();
            assertEquals(ZoneTransferCoordinator.Outcome.CLOSED, get(t.result()).outcome());
            assertThrows(ExecutionException.class, () -> f.begin(8, TIMEOUT));
            assertEquals(0, f.coordinator.pendingCount());
        }
    }
    @Test void deadlineDuringReleaseReportsRecoveryAndLateReleaseCannotCommit() throws Exception {
        try (var f = new Fixture(16, 1)) {
            f.source.releasing = new CountDownLatch(1); f.source.releaseOwnership = new CountDownLatch(1);
            var t = f.begin(7, Duration.ofMillis(200)); await(f.source.releasing);
            assertEquals(ZoneTransferCoordinator.Outcome.RECOVERY_REQUIRED, get(t.result()).outcome());
            assertThrows(IllegalStateException.class, () -> f.coordinator.acknowledgeRecovery(t));
            f.source.releaseOwnership.countDown();
            get(f.source.actor.tell(() -> {})); get(f.target.actor.tell(() -> {}));
            assertEquals(List.of("freeze", "prepare", "release"), f.events);
            assertEquals(1, f.coordinator.pendingCount());
        }
    }
    @Test void targetStopsAfterPreparationAndRejectedCommitCannotRestoreSource() throws Exception {
        try (var f = new Fixture(16, 1)) {
            f.blockPrepare(); var t = f.begin(7, TIMEOUT); await(f.target.preparing);
            var stopped = f.target.actor.stop(); f.target.releasePrepare.countDown(); get(stopped);
            assertEquals(ZoneTransferCoordinator.Outcome.RECOVERY_REQUIRED, get(t.result()).outcome());
            assertFalse(get(f.source.actor.call(() -> f.source.active.containsKey(7L))));
            assertEquals(List.of("freeze", "prepare", "release"), f.events);
        }
    }
    @Test void invalidPayloadAndMissingZoneDoNotFreezeSource() throws Exception {
        try (var f = new Fixture(16, 1)) {
            get(f.source.actor.tell(() -> {
                assertThrows(IllegalArgumentException.class, () -> f.coordinator.begin(
                    "source", "absent", 7, new byte[1], TIMEOUT));
                assertThrows(IllegalArgumentException.class, () -> f.coordinator.begin(
                    "source", "target", 7, new byte[65_537], TIMEOUT));
                assertThrows(IllegalArgumentException.class, () -> f.coordinator.begin(
                    "source", "target", 7, new byte[1], Duration.ZERO));
            }));
            assertEquals(0, f.coordinator.pendingCount()); assertTrue(f.events.isEmpty());
        }
    }}
