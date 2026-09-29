package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HealthRegistryTest {
    @Test
    void aggregatesHealthyAndDegradedChecksInStableOrder() throws Exception {
        var registry = new HealthRegistry(3);
        registry.register("z-db", () -> CompletableFuture.completedFuture(HealthRegistry.State.UP));
        registry.register("a-cache", () -> CompletableFuture.completedFuture(HealthRegistry.State.DEGRADED));

        var snapshot = registry.check(Duration.ofSeconds(1), 10).toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(HealthRegistry.State.DEGRADED, snapshot.overall());
        assertEquals("a-cache", snapshot.components().getFirst().name());
        assertEquals(HealthRegistry.State.DEGRADED, snapshot.components().getFirst().state());
    }

    @Test
    void timeoutAndExceptionDoNotBlockOtherChecks() throws Exception {
        var registry = new HealthRegistry(3);
        registry.register("slow", () -> new CompletableFuture<>());
        registry.register("broken", () -> { throw new IllegalStateException("boom"); });

        var snapshot = registry.check(Duration.ofMillis(20), 20).toCompletableFuture().get(1, TimeUnit.SECONDS);
        assertEquals(HealthRegistry.State.DEGRADED, snapshot.overall());
        assertEquals(HealthRegistry.State.UNKNOWN, snapshot.components().get(0).state());
        assertTrue(snapshot.components().get(0).detail().contains("exception"));
        assertEquals(HealthRegistry.State.UNKNOWN, snapshot.components().get(1).state());
    }

    @Test
    void downDominatesAndCapacityAndDuplicateNamesAreBounded() {
        var registry = new HealthRegistry(1);
        registry.register("db", () -> CompletableFuture.completedFuture(HealthRegistry.State.DOWN));
        assertThrows(IllegalStateException.class,
                () -> registry.register("db", () -> CompletableFuture.completedFuture(HealthRegistry.State.UP)));
        assertThrows(IllegalStateException.class,
                () -> registry.register("cache", () -> CompletableFuture.completedFuture(HealthRegistry.State.UP)));
        assertEquals(1, registry.size());
    }
}
