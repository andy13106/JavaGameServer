package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CrossProcessMigrationCoordinatorTest {
    @Test
    void acceptedMigrationChangesOwnershipOnlyAfterPreparation() throws Exception {
        var events = new ArrayList<String>();
        var source = source(events, true, false);
        var target = target(events, true, false);
        try (var coordinator = new CrossProcessMigrationCoordinator()) {
            var migration = coordinator.begin(request("accepted", 2_000), source, target);
            var result = migration.result().toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertTrue(result.accepted());
            assertEquals(CrossProcessMigrationCoordinator.Phase.DONE, migration.phase());
            assertEquals(List.of("freeze", "prepare", "release", "commit"), events);
            assertEquals(0, coordinator.pendingCount());
        }
    }

    @Test
    void rejectedPreparationCancelsTargetAndResumesSource() throws Exception {
        var events = new ArrayList<String>();
        try (var coordinator = new CrossProcessMigrationCoordinator()) {
            var migration = coordinator.begin(request("reject", 2_000), source(events, true, false),
                    target(events, false, false));
            var result = migration.result().toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertEquals(CrossProcessMigrationCoordinator.Outcome.REJECTED, result.outcome());
            assertEquals(List.of("freeze", "prepare", "cancel", "resume"), events);
            assertEquals(0, coordinator.pendingCount());
        }
    }

    @Test
    void commitFailureStaysInRecoveryUntilAcknowledged() throws Exception {
        var events = new ArrayList<String>();
        try (var coordinator = new CrossProcessMigrationCoordinator()) {
            var migration = coordinator.begin(request("recovery", 2_000), source(events, true, false),
                    target(events, true, true));
            var result = migration.result().toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertEquals(CrossProcessMigrationCoordinator.Outcome.RECOVERY_REQUIRED, result.outcome());
            assertEquals(CrossProcessMigrationCoordinator.Phase.RECOVERY_REQUIRED, migration.phase());
            assertEquals(List.of("freeze", "prepare", "release", "commit"), events);
            assertEquals(1, coordinator.pendingCount());
            coordinator.acknowledgeRecovery(migration);
            assertEquals(0, coordinator.pendingCount());
        }
    }

    @Test
    void timeoutDuringPreparationRetainsRecoveryWhenTargetIsUncertain() throws Exception {
        var events = new ArrayList<String>();
        var pendingPrepare = new CompletableFuture<Boolean>();
        var target = new CrossProcessMigrationCoordinator.Target() {
            public java.util.concurrent.CompletionStage<Boolean> prepare(CrossProcessMigrationCoordinator.Request request) {
                events.add("prepare"); return pendingPrepare;
            }
            public java.util.concurrent.CompletionStage<Void> commit(CrossProcessMigrationCoordinator.Request request) {
                events.add("commit"); return CompletableFuture.completedFuture(null);
            }
            public java.util.concurrent.CompletionStage<Void> cancel(CrossProcessMigrationCoordinator.Request request) {
                events.add("cancel"); return CompletableFuture.completedFuture(null);
            }
        };
        try (var coordinator = new CrossProcessMigrationCoordinator()) {
            var migration = coordinator.begin(request("timeout", 60), source(events, true, false), target);
            var result = migration.result().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(CrossProcessMigrationCoordinator.Outcome.RECOVERY_REQUIRED, result.outcome());
            assertEquals(CrossProcessMigrationCoordinator.Phase.RECOVERY_REQUIRED, migration.phase());
            assertEquals(List.of("freeze", "prepare"), events);
            assertEquals(1, coordinator.pendingCount());
            assertThrows(IllegalStateException.class, () -> coordinator.acknowledgeRecovery(migration));
            pendingPrepare.complete(false);
            coordinator.acknowledgeRecovery(migration);
            assertEquals(0, coordinator.pendingCount());
        }
    }

    private static CrossProcessMigrationCoordinator.Request request(String id, long timeoutMillis) {
        return new CrossProcessMigrationCoordinator.Request(id, "actor-1", "game-a", "game-b", 7,
                new byte[]{1, 2, 3}, System.currentTimeMillis() + timeoutMillis);
    }

    private static CrossProcessMigrationCoordinator.Source source(List<String> events, boolean freeze,
                                                                   boolean releaseFails) {
        return new CrossProcessMigrationCoordinator.Source() {
            public java.util.concurrent.CompletionStage<Boolean> freeze(CrossProcessMigrationCoordinator.Request request) {
                events.add("freeze"); return CompletableFuture.completedFuture(freeze);
            }
            public java.util.concurrent.CompletionStage<Void> release(CrossProcessMigrationCoordinator.Request request) {
                events.add("release");
                return releaseFails ? CompletableFuture.failedFuture(new IllegalStateException("release"))
                        : CompletableFuture.completedFuture(null);
            }
            public java.util.concurrent.CompletionStage<Void> resume(CrossProcessMigrationCoordinator.Request request) {
                events.add("resume"); return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static CrossProcessMigrationCoordinator.Target target(List<String> events, boolean prepare,
                                                                   boolean commitFails) {
        return new CrossProcessMigrationCoordinator.Target() {
            public java.util.concurrent.CompletionStage<Boolean> prepare(CrossProcessMigrationCoordinator.Request request) {
                events.add("prepare"); return CompletableFuture.completedFuture(prepare);
            }
            public java.util.concurrent.CompletionStage<Void> commit(CrossProcessMigrationCoordinator.Request request) {
                events.add("commit");
                return commitFails ? CompletableFuture.failedFuture(new IllegalStateException("commit"))
                        : CompletableFuture.completedFuture(null);
            }
            public java.util.concurrent.CompletionStage<Void> cancel(CrossProcessMigrationCoordinator.Request request) {
                events.add("cancel"); return CompletableFuture.completedFuture(null);
            }
        };
    }
}