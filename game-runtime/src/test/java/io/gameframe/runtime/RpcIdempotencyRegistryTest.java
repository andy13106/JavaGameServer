package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class RpcIdempotencyRegistryTest {
    @Test
    void concurrentRetryJoinsTheOriginalOperation() {
        var registry = new RpcIdempotencyRegistry<String>();
        var operation = new CompletableFuture<String>();
        var first = registry.execute("command-1", 0, () -> operation);
        var retry = registry.execute("command-1", 1, () -> {
            fail("retry must not invoke the operation");
            return operation;
        });

        assertTrue(first.owner());
        assertFalse(retry.owner());
        operation.complete("done");
        assertEquals("done", retry.stage().toCompletableFuture().join());
    }

    @Test
    void completedCommandIsRetainedThenExpires() {
        var registry = new RpcIdempotencyRegistry<String>(new RpcIdempotencyRegistry.Limits(4, 10));
        var first = registry.execute("command-1", 0, () -> CompletableFuture.completedFuture("done"));
        assertEquals("done", first.stage().toCompletableFuture().join());
        assertFalse(registry.execute("command-1", 5, () -> CompletableFuture.completedFuture("new")).owner());
        assertEquals(1, registry.purge(10));
        assertTrue(registry.execute("command-1", 10, () -> CompletableFuture.completedFuture("new")).owner());
    }

    @Test
    void capacityAndCloseBoundTheRegistry() {
        var registry = new RpcIdempotencyRegistry<String>(new RpcIdempotencyRegistry.Limits(1, 100));
        var pending = registry.execute("command-1", 0, CompletableFuture::new);
        assertThrows(IllegalStateException.class,
                () -> registry.execute("command-2", 1, () -> CompletableFuture.completedFuture("x")));
        registry.close();
        assertThrows(Exception.class, () -> pending.stage().toCompletableFuture().join());
        assertThrows(IllegalStateException.class,
                () -> registry.execute("command-3", 2, () -> CompletableFuture.completedFuture("x")));
    }
}