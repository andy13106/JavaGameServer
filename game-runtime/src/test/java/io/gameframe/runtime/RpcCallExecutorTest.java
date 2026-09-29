package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RpcCallExecutorTest {
    @Test
    void retriesOnlyIdempotentCallAndReusesStableCommand() {
        var attempts = new AtomicInteger();
        try (var executor = new RpcCallExecutor(policy(3, 5), 1, "rpc-test")) {
            var result = executor.execute("game-1", "command-1", RpcCallExecutor.RetryMode.IDEMPOTENT,
                    System.currentTimeMillis() + 1_000,
                    () -> attempts.incrementAndGet() == 1
                            ? CompletableFuture.failedFuture(new IOException("temporary"))
                            : CompletableFuture.completedFuture("ok"),
                    IOException.class::isInstance);

            assertEquals("ok", result.toCompletableFuture().join());
            assertEquals(2, attempts.get());
            assertEquals(1, executor.stats().retries());
        }
    }

    @Test
    void nonIdempotentCallNeverRetries() {
        var attempts = new AtomicInteger();
        try (var executor = new RpcCallExecutor(policy(3, 5), 1, "rpc-test")) {
            var result = executor.execute("game-1", "command-1", RpcCallExecutor.RetryMode.NEVER,
                    System.currentTimeMillis() + 1_000,
                    () -> {
                        attempts.incrementAndGet();
                        return CompletableFuture.failedFuture(new IOException("temporary"));
                    }, IOException.class::isInstance);

            assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
            assertEquals(1, attempts.get());
        }
    }

    @Test
    void totalDeadlineStopsWaitingForLateCompletion() {
        var late = new CompletableFuture<String>();
        try (var executor = new RpcCallExecutor(policy(2, 5), 1, "rpc-test")) {
            var result = executor.execute("game-1", "command-1", RpcCallExecutor.RetryMode.NEVER,
                    System.currentTimeMillis() + 40, () -> late, ignored -> false);
            var failure = assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
            assertTrue(failure.getCause() instanceof RpcCallExecutor.RpcDeadlineException
                    || failure.getCause() instanceof RpcCallExecutor.AttemptTimeoutException);
            late.complete("too-late");
            assertTrue(result.toCompletableFuture().isCompletedExceptionally());
        }
    }

    @Test
    void circuitOpensAllowsOneProbeAndClosesOnSuccess() throws Exception {
        try (var executor = new RpcCallExecutor(policy(1, 1), 1, "rpc-test")) {
            var failed = executor.execute("game-1", "command-1", RpcCallExecutor.RetryMode.NEVER,
                    System.currentTimeMillis() + 1_000,
                    () -> CompletableFuture.failedFuture(new IOException("down")),
                    IOException.class::isInstance);
            assertThrows(CompletionException.class, () -> failed.toCompletableFuture().join());
            assertEquals(RpcCallExecutor.CircuitState.OPEN, executor.circuitState("game-1"));

            var rejected = executor.execute("game-1", "command-2", RpcCallExecutor.RetryMode.NEVER,
                    System.currentTimeMillis() + 1_000,
                    () -> CompletableFuture.completedFuture("unexpected"), ignored -> false);
            var rejection = assertThrows(CompletionException.class, () -> rejected.toCompletableFuture().join());
            assertInstanceOf(RpcCallExecutor.CircuitOpenException.class, rejection.getCause());

            Thread.sleep(60);
            assertEquals(RpcCallExecutor.CircuitState.HALF_OPEN, executor.circuitState("game-1"));
            var probe = executor.execute("game-1", "command-3", RpcCallExecutor.RetryMode.NEVER,
                    System.currentTimeMillis() + 1_000,
                    () -> CompletableFuture.completedFuture("ok"), ignored -> false);
            assertEquals("ok", probe.toCompletableFuture().join());
            assertEquals(RpcCallExecutor.CircuitState.CLOSED, executor.circuitState("game-1"));
        }
    }

    @Test
    void closeFailsActiveCallsAndReleasesTimers() {
        var executor = new RpcCallExecutor(policy(1, 5), 1, "rpc-test");
        var result = executor.execute("game-1", "command-1", RpcCallExecutor.RetryMode.NEVER,
                System.currentTimeMillis() + 10_000, CompletableFuture::new, ignored -> false);
        executor.close();
        assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
        assertEquals(0, executor.stats().pendingTimers());
        assertTrue(executor.stats().closed());
    }

    private static RpcCallExecutor.Policy policy(int attempts, int threshold) {
        return new RpcCallExecutor.Policy(attempts, Duration.ofMillis(200), Duration.ofMillis(1),
                Duration.ofMillis(5), threshold, Duration.ofMillis(40), 8, 32);
    }
}