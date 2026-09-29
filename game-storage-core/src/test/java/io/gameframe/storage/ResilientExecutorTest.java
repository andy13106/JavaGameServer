package io.gameframe.storage;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class ResilientExecutorTest {
    private static ResiliencePolicy policy(int attempts, Duration timeout, int threshold, Duration open) {
        return new ResiliencePolicy(attempts, timeout, Duration.ofMillis(10),
            Duration.ofMillis(40), threshold, open);
    }
    private static Throwable failure(CompletionStage<?> stage) {
        var error = assertThrows(ExecutionException.class,
            () -> stage.toCompletableFuture().get(3, TimeUnit.SECONDS));
        return error.getCause();
    }

    @Test void retriesOnlyWithExplicitPermissionAndStopsAtConfiguredAttemptLimit() throws Exception {
        try (var executor = new ResilientExecutor(
            policy(3, Duration.ofSeconds(1), 10, Duration.ofSeconds(1)), 1, 16, "retry-test")) {
            var safeAttempts = new AtomicInteger();
            String value = executor.execute("safe", ResilientExecutor.RetryPermission.IDEMPOTENT, () -> {
                int current = safeAttempts.incrementAndGet();
                return current < 3 ? CompletableFuture.failedFuture(new IOException("temporary-" + current))
                    : CompletableFuture.completedFuture("done");
            }, IOException.class::isInstance).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals("done", value);
            assertEquals(3, safeAttempts.get());

            var unsafeAttempts = new AtomicInteger();
            assertInstanceOf(IOException.class, failure(executor.execute(
                "unsafe", ResilientExecutor.RetryPermission.NEVER, () -> {
                    unsafeAttempts.incrementAndGet();
                    return CompletableFuture.failedFuture(new IOException("do not retry"));
                }, IOException.class::isInstance)));
            assertEquals(1, unsafeAttempts.get());

            var exhausted = new AtomicInteger();
            assertInstanceOf(IOException.class, failure(executor.execute(
                "exhausted", ResilientExecutor.RetryPermission.IDEMPOTENT, () -> {
                    exhausted.incrementAndGet();
                    return CompletableFuture.failedFuture(new IOException("still down"));
                }, IOException.class::isInstance)));
            assertEquals(3, exhausted.get());
            assertEquals(4, executor.stats().retries());
        }
    }

    @Test void timeoutCanRetrySafeOperationAndLateCompletionCannotReplaceResult() throws Exception {
        try (var executor = new ResilientExecutor(
            policy(2, Duration.ofMillis(60), 5, Duration.ofSeconds(1)), 1, 16, "timeout-test")) {
            var attempts = new AtomicInteger();
            var late = new CompletableFuture<String>();
            var result = executor.execute("timeout", ResilientExecutor.RetryPermission.IDEMPOTENT, () ->
                attempts.incrementAndGet() == 1 ? late : CompletableFuture.completedFuture("second"),
                ResilientExecutor.AttemptTimeoutException.class::isInstance);
            assertEquals("second", result.toCompletableFuture().get(3, TimeUnit.SECONDS));
            late.complete("late-first");
            assertEquals("second", result.toCompletableFuture().get());
            assertEquals(2, attempts.get());
            assertEquals(1, executor.stats().timeouts());
        }
    }

    @Test void nonRetryableFailureDoesNotRetryOrOpenCircuit() {
        try (var executor = new ResilientExecutor(
            policy(3, Duration.ofSeconds(1), 1, Duration.ofMillis(100)), 1, 16, "classification-test")) {
            var attempts = new AtomicInteger();
            assertInstanceOf(IllegalArgumentException.class, failure(executor.execute(
                "validation", ResilientExecutor.RetryPermission.IDEMPOTENT, () -> {
                    attempts.incrementAndGet();
                    return CompletableFuture.failedFuture(new IllegalArgumentException("invalid command"));
                }, IOException.class::isInstance)));
            assertEquals(1, attempts.get());
            assertEquals(ResilientExecutor.CircuitState.CLOSED, executor.circuitState("validation"));
        }
    }

    @Test void circuitOpensRejectsCallsAndUsesSingleHalfOpenProbe() throws Exception {
        try (var executor = new ResilientExecutor(
            policy(1, Duration.ofSeconds(1), 2, Duration.ofMillis(120)), 1, 16, "breaker-test")) {
            for (int i = 0; i < 2; i++) {
                assertInstanceOf(IOException.class, failure(executor.execute(
                    "redis", ResilientExecutor.RetryPermission.NEVER,
                    () -> CompletableFuture.failedFuture(new IOException("down")),
                    IOException.class::isInstance)));
            }
            assertEquals(ResilientExecutor.CircuitState.OPEN, executor.circuitState("redis"));
            assertInstanceOf(ResilientExecutor.CircuitOpenException.class, failure(executor.execute(
                "redis", ResilientExecutor.RetryPermission.NEVER,
                () -> fail("open circuit invoked operation"), IOException.class::isInstance)));
            Thread.sleep(160);
            assertEquals(ResilientExecutor.CircuitState.HALF_OPEN, executor.circuitState("redis"));

            var probe = new CompletableFuture<String>();
            var probeResult = executor.execute("redis", ResilientExecutor.RetryPermission.NEVER,
                () -> probe, IOException.class::isInstance);
            assertInstanceOf(ResilientExecutor.CircuitOpenException.class, failure(executor.execute(
                "redis", ResilientExecutor.RetryPermission.NEVER,
                () -> fail("second half-open probe invoked"), IOException.class::isInstance)));
            probe.complete("healthy");
            assertEquals("healthy", probeResult.toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertEquals(ResilientExecutor.CircuitState.CLOSED, executor.circuitState("redis"));
        }
    }

    @Test void nonRetryableHalfOpenProbeClosesCircuitInsteadOfSticking() throws Exception {
        try (var executor = new ResilientExecutor(
            policy(1, Duration.ofSeconds(1), 1, Duration.ofMillis(80)), 1, 16, "half-open-test")) {
            assertInstanceOf(IOException.class, failure(executor.execute(
                "mongo", ResilientExecutor.RetryPermission.NEVER,
                () -> CompletableFuture.failedFuture(new IOException("down")),
                IOException.class::isInstance)));
            Thread.sleep(120);
            assertInstanceOf(IllegalArgumentException.class, failure(executor.execute(
                "mongo", ResilientExecutor.RetryPermission.NEVER,
                () -> CompletableFuture.failedFuture(new IllegalArgumentException("bad request")),
                IOException.class::isInstance)));
            assertEquals(ResilientExecutor.CircuitState.CLOSED, executor.circuitState("mongo"));
            assertEquals("ok", executor.execute("mongo", ResilientExecutor.RetryPermission.NEVER,
                () -> CompletableFuture.completedFuture("ok"),
                IOException.class::isInstance).toCompletableFuture().get(3, TimeUnit.SECONDS));
        }
    }
    @Test void circuitNamesHaveAHardCapacityLimit() throws Exception {
        try (var executor = new ResilientExecutor(
            policy(1, Duration.ofSeconds(1), 5, Duration.ofSeconds(1)),
            1, 8, 1, "circuit-capacity-test")) {
            assertEquals("one", executor.execute("fixed-backend", ResilientExecutor.RetryPermission.NEVER,
                () -> CompletableFuture.completedFuture("one"), ignored -> false)
                .toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertInstanceOf(RejectedExecutionException.class, failure(executor.execute(
                "player-id-must-not-be-a-circuit", ResilientExecutor.RetryPermission.NEVER,
                () -> fail("operation started after circuit capacity exhausted"), ignored -> false)));
            assertEquals(1, executor.stats().circuits());
        }
    }
    @Test void timerCapacityIsBoundedAndCloseFailsPendingCalls() throws Exception {
        var executor = new ResilientExecutor(
            policy(1, Duration.ofSeconds(2), 5, Duration.ofSeconds(1)), 1, 1, "capacity-test");
        var first = new CompletableFuture<String>();
        var firstResult = executor.execute("one", ResilientExecutor.RetryPermission.NEVER,
            () -> first, ignored -> false);
        var secondStarted = new AtomicInteger();
        var secondResult = executor.execute("two", ResilientExecutor.RetryPermission.NEVER, () -> {
            secondStarted.incrementAndGet();
            return new CompletableFuture<>();
        }, ignored -> false);
        assertInstanceOf(RejectedExecutionException.class, failure(secondResult));
        assertEquals(0, secondStarted.get());
        executor.close();
        assertInstanceOf(RejectedExecutionException.class, failure(firstResult));
        assertThrows(ExecutionException.class, () -> executor.execute(
            "closed", ResilientExecutor.RetryPermission.NEVER,
            () -> CompletableFuture.completedFuture("never"), ignored -> false).toCompletableFuture().get());
        assertTrue(executor.stats().closed());
    }
}