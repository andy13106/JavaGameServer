package io.gameframe.scene;

import io.gameframe.runtime.CrossProcessMigrationCoordinator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CrossProcessPlayerMigrationTest {
    @Test
    void publishesRouteOnlyAfterTargetCommit() throws Exception {
        var events = new ArrayList<String>();
        var source = node(events, "source");
        var target = node(events, "target");
        var owner = (CrossProcessPlayerMigration.RouteOwner) (player, instance, token) -> {
            events.add("route:" + player + ":" + instance + ":" + token);
            return CompletableFuture.completedFuture(null);
        };
        try (var coordinator = new CrossProcessMigrationCoordinator()) {
            var migration = new CrossProcessPlayerMigration(coordinator, owner).begin(request(), source, target);
            var result = migration.result().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertTrue(result.accepted());
            assertEquals(List.of("source:freeze", "target:prepare", "source:release",
                    "target:commit", "route:player-7:game-b:41"), events);
        }
    }

    @Test
    void routePublishFailureLeavesRecoveryAndNeverResumesSource() throws Exception {
        var events = new ArrayList<String>();
        var owner = (CrossProcessPlayerMigration.RouteOwner) (player, instance, token) -> {
            events.add("route");
            return CompletableFuture.failedFuture(new IllegalStateException("route unavailable"));
        };
        try (var coordinator = new CrossProcessMigrationCoordinator()) {
            var api = new CrossProcessPlayerMigration(coordinator, owner);
            var migration = api.begin(request(), node(events, "source"), node(events, "target"));
            var result = migration.result().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(CrossProcessMigrationCoordinator.Outcome.RECOVERY_REQUIRED, result.outcome());
            assertFalse(events.contains("source:resume"));
            assertEquals(1, coordinator.pendingCount());
            coordinator.acknowledgeRecovery(migration);
        }
    }

    private static CrossProcessPlayerMigration.Request request() {
        return new CrossProcessPlayerMigration.Request("player-7", "game-a", "game-b", 41,
                new byte[]{1, 2, 3}, System.currentTimeMillis() + 5_000);
    }

    private static CrossProcessPlayerMigration.Node node(List<String> events, String name) {
        return new CrossProcessPlayerMigration.Node() {
            public CompletionStage<Boolean> freeze(CrossProcessPlayerMigration.Request request) {
                events.add(name + ":freeze"); return CompletableFuture.completedFuture(true);
            }
            public CompletionStage<Void> release(CrossProcessPlayerMigration.Request request) {
                events.add(name + ":release"); return CompletableFuture.completedFuture(null);
            }
            public CompletionStage<Void> resume(CrossProcessPlayerMigration.Request request) {
                events.add(name + ":resume"); return CompletableFuture.completedFuture(null);
            }
            public CompletionStage<Boolean> prepare(CrossProcessPlayerMigration.Request request) {
                events.add(name + ":prepare"); return CompletableFuture.completedFuture(true);
            }
            public CompletionStage<Void> commit(CrossProcessPlayerMigration.Request request) {
                events.add(name + ":commit"); return CompletableFuture.completedFuture(null);
            }
            public CompletionStage<Void> cancel(CrossProcessPlayerMigration.Request request) {
                events.add(name + ":cancel"); return CompletableFuture.completedFuture(null);
            }
        };
    }
}