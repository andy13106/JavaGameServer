package io.gameframe.storage.redis;

import io.gameframe.storage.*;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Applies a finite resilience policy only to Redis idempotency operations that are replay-safe
 * with the same key, token and result.
 */
public final class ResilientRedisIdempotency {
    private final RedisStore store;
    private final ResilientExecutor resilience;
    private final String circuit;

    public ResilientRedisIdempotency(RedisStore store, ResilientExecutor resilience, String circuit) {
        this.store = Objects.requireNonNull(store);
        this.resilience = Objects.requireNonNull(resilience);
        if (circuit == null || circuit.isBlank()) throw new IllegalArgumentException("circuit required");
        this.circuit = circuit;
    }

    public CompletionStage<RedisStore.IdempotencyState> claim(
        String key, String token, Duration pendingTtl) {
        return resilience.execute(circuit, ResilientExecutor.RetryPermission.IDEMPOTENT,
            () -> store.claimIdempotency(key, token, pendingTtl),
            ResilientRedisIdempotency::retryable);
    }

    public CompletionStage<RedisStore.CompletionStatus> complete(
        String key, String token, String result, Duration resultTtl) {
        return resilience.execute(circuit, ResilientExecutor.RetryPermission.IDEMPOTENT,
            () -> store.completeIdempotency(key, token, result, resultTtl),
            ResilientRedisIdempotency::retryable);
    }

    public CompletionStage<RedisStore.IdempotencyState> get(String key) {
        return resilience.execute(circuit, ResilientExecutor.RetryPermission.IDEMPOTENT,
            () -> store.getIdempotency(key), ResilientRedisIdempotency::retryable);
    }

    private static boolean retryable(Throwable error) {
        return error instanceof StorageException ||
            error instanceof ResilientExecutor.AttemptTimeoutException;
    }
}