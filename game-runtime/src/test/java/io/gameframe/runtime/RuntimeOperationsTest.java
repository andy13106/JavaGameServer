package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class RuntimeOperationsTest {
    @Test
    void snapshotCombinesHealthMetricsAndDrainState() throws Exception {
        var health = new HealthRegistry(2);
        health.register("database", () -> CompletableFuture.completedFuture(HealthRegistry.State.UP));
        var metrics = new RuntimeMetrics();
        metrics.increment("rpc_calls", 3);
        var drain = new GracefulDrain();
        var permit = drain.acquire();
        try {
            var operations = new RuntimeOperations(health, metrics, drain);
            var snapshot = operations.snapshot(Duration.ofSeconds(1), 1234).toCompletableFuture().get();
            assertEquals(1234, snapshot.capturedAtMillis());
            assertEquals(HealthRegistry.State.UP, snapshot.health().overall());
            assertEquals(3, snapshot.metrics().counters().get("rpc_calls"));
            assertEquals(GracefulDrain.State.ACTIVE, snapshot.drainState());
            assertEquals(1, snapshot.inFlight());
            assertTrue(operations.prometheus().contains("gameframe_drain_in_flight 1"));
        } finally {
            permit.close();
        }
    }

    @Test
    void healthFailureRemainsObservableWithoutChangingMetrics() throws Exception {
        var health = new HealthRegistry(1);
        health.register("cache", () -> CompletableFuture.completedFuture(HealthRegistry.State.DOWN));
        var metrics = new RuntimeMetrics();
        var operations = new RuntimeOperations(health, metrics, new GracefulDrain());
        var snapshot = operations.snapshot(Duration.ofSeconds(1), 9).toCompletableFuture().get();
        assertEquals(HealthRegistry.State.DOWN, snapshot.health().overall());
        assertTrue(snapshot.metrics().counters().isEmpty());
    }
}