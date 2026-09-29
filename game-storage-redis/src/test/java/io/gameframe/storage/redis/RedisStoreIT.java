package io.gameframe.storage.redis;

import io.gameframe.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.time.Duration;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_REDIS_URI", matches=".+")
class RedisStoreIT {
    static String key() { return "gameframe:it:idempotency:" + UUID.randomUUID(); }

    @Test void ttlAndTokenCheckedLeaseRelease() throws Exception {
        try (var store = new RedisStore(System.getenv("GAME_TEST_REDIS_URI"), 2, 32)) {
            String key = "gameframe:it:lease:" + UUID.randomUUID();
            assertTrue(store.setIfAbsent(key, "owner-A", Duration.ofSeconds(30)).toCompletableFuture().get());
            assertFalse(store.setIfAbsent(key, "owner-B", Duration.ofSeconds(30)).toCompletableFuture().get());
            assertFalse(store.releaseLease(key, "owner-B").toCompletableFuture().get());
            assertEquals("owner-A", store.get(key).toCompletableFuture().get());
            assertTrue(store.releaseLease(key, "owner-A").toCompletableFuture().get());
            assertNull(store.get(key).toCompletableFuture().get());
        }
    }

    @Test void claimCompleteAndReplayReturnTheStoredResult() throws Exception {
        try (var store = new RedisStore(System.getenv("GAME_TEST_REDIS_URI"), 2, 32)) {
            String key = key();
            assertEquals(RedisStore.IdempotencyStatus.ACQUIRED,
                store.claimIdempotency(key, "owner-A", Duration.ofSeconds(30)).toCompletableFuture().get().status());
            var competing = store.claimIdempotency(key, "owner-B", Duration.ofSeconds(30)).toCompletableFuture().get();
            assertEquals(RedisStore.IdempotencyStatus.IN_PROGRESS, competing.status());
            assertEquals(RedisStore.CompletionStatus.APPLIED,
                store.completeIdempotency(key, "owner-A", "订单已处理:42", Duration.ofSeconds(30)).toCompletableFuture().get());
            var replay = store.claimIdempotency(key, "owner-C", Duration.ofSeconds(30)).toCompletableFuture().get();
            assertEquals(RedisStore.IdempotencyStatus.COMPLETED, replay.status());
            assertEquals("订单已处理:42", replay.result());
            assertEquals(replay.status(), store.getIdempotency(key).toCompletableFuture().get().status());
            assertEquals(RedisStore.CompletionStatus.ALREADY_COMPLETED,
                store.completeIdempotency(key, "owner-A", "订单已处理:42", Duration.ofSeconds(30)).toCompletableFuture().get());
            assertEquals(RedisStore.CompletionStatus.REJECTED,
                store.completeIdempotency(key, "owner-C", "覆盖结果", Duration.ofSeconds(30)).toCompletableFuture().get());
        }
    }

    @Test void expiredClaimLetsNewOwnerProceedAndOldOwnerCannotComplete() throws Exception {
        try (var store = new RedisStore(System.getenv("GAME_TEST_REDIS_URI"), 2, 32)) {
            String key = key();
            assertEquals(RedisStore.IdempotencyStatus.ACQUIRED,
                store.claimIdempotency(key, "old", Duration.ofMillis(100)).toCompletableFuture().get().status());
            Thread.sleep(180);
            assertEquals(RedisStore.IdempotencyStatus.ABSENT,
                store.getIdempotency(key).toCompletableFuture().get().status());
            assertEquals(RedisStore.IdempotencyStatus.ACQUIRED,
                store.claimIdempotency(key, "new", Duration.ofSeconds(30)).toCompletableFuture().get().status());
            assertEquals(RedisStore.CompletionStatus.REJECTED,
                store.completeIdempotency(key, "old", "stale", Duration.ofSeconds(30)).toCompletableFuture().get());
            assertEquals(RedisStore.CompletionStatus.APPLIED,
                store.completeIdempotency(key, "new", "fresh", Duration.ofSeconds(30)).toCompletableFuture().get());
            assertEquals("fresh", store.getIdempotency(key).toCompletableFuture().get().result());
        }
    }

    @Test void completedResultExpiresAndValidationHappensBeforeRedisCommands() throws Exception {
        try (var store = new RedisStore(System.getenv("GAME_TEST_REDIS_URI"), 2, 32)) {
            String key = key();
            assertThrows(IllegalArgumentException.class,
                () -> store.claimIdempotency(key, "x", Duration.ZERO));
            assertThrows(IllegalArgumentException.class,
                () -> store.completeIdempotency(key, "x", null, Duration.ofSeconds(1)));
            assertEquals(RedisStore.CompletionStatus.MISSING,
                store.completeIdempotency(key, "x", "missing", Duration.ofSeconds(1)).toCompletableFuture().get());
            assertEquals(RedisStore.IdempotencyStatus.ACQUIRED,
                store.claimIdempotency(key, "x", Duration.ofSeconds(1)).toCompletableFuture().get().status());
            assertEquals(RedisStore.CompletionStatus.APPLIED,
                store.completeIdempotency(key, "x", "short-lived", Duration.ofMillis(100)).toCompletableFuture().get());
            Thread.sleep(180);
            assertEquals(RedisStore.IdempotencyStatus.ABSENT,
                store.getIdempotency(key).toCompletableFuture().get().status());
        }
    }

    @Test void resilientAdapterUsesReplaySafeTokenAndResultSemantics() throws Exception {
        var policy = new ResiliencePolicy(3, Duration.ofSeconds(1), Duration.ofMillis(10),
            Duration.ofMillis(50), 3, Duration.ofSeconds(1));
        try (var store = new RedisStore(System.getenv("GAME_TEST_REDIS_URI"), 2, 32);
             var resilience = new ResilientExecutor(policy, 1, 32, "redis-resilience-it")) {
            var api = new ResilientRedisIdempotency(store, resilience, "redis-idempotency");
            String key = key();
            assertEquals(RedisStore.IdempotencyStatus.ACQUIRED,
                api.claim(key, "same-token", Duration.ofMillis(400)).toCompletableFuture().get().status());
            Thread.sleep(250);
            assertEquals(RedisStore.IdempotencyStatus.ACQUIRED,
                api.claim(key, "same-token", Duration.ofSeconds(2)).toCompletableFuture().get().status());
            Thread.sleep(250);
            assertEquals(RedisStore.IdempotencyStatus.IN_PROGRESS,
                api.claim(key, "different-token", Duration.ofSeconds(30)).toCompletableFuture().get().status());
            assertEquals(RedisStore.CompletionStatus.APPLIED,
                api.complete(key, "same-token", "result-7", Duration.ofSeconds(30)).toCompletableFuture().get());
            assertEquals(RedisStore.CompletionStatus.ALREADY_COMPLETED,
                api.complete(key, "same-token", "result-7", Duration.ofSeconds(30)).toCompletableFuture().get());
            assertEquals("result-7", api.get(key).toCompletableFuture().get().result());
            assertEquals(6, resilience.stats().successes());
            assertEquals(0, resilience.stats().retries());
        }
    }
    @Test void migrationJournalUsesVersionFencingAndRecoveryListing() throws Exception {
        try (var store = new RedisStore(System.getenv("GAME_TEST_REDIS_URI"), 2, 32);
             var journal = new RedisMigrationJournal(store, "gameframe:it:migration:" + UUID.randomUUID(), Duration.ofMinutes(5), 1024)) {
            String id = "migration-" + UUID.randomUUID();
            var initial = new io.gameframe.runtime.MigrationJournal.Entry(id, "actor-1", "game-a", "game-b", 7,
                    io.gameframe.runtime.CrossProcessMigrationCoordinator.Phase.PREPARING, 1,
                    System.currentTimeMillis(), new byte[]{1, 2, 3});
            assertTrue(journal.create(initial).toCompletableFuture().get());
            var recovery = new io.gameframe.runtime.MigrationJournal.Entry(id, "actor-1", "game-a", "game-b", 7,
                    io.gameframe.runtime.CrossProcessMigrationCoordinator.Phase.RECOVERY_REQUIRED, 2,
                    System.currentTimeMillis(), new byte[]{4, 5});
            assertTrue(journal.compareAndSet(id, 1, recovery).toCompletableFuture().get());
            var stale = new io.gameframe.runtime.MigrationJournal.Entry(id, "actor-1", "game-a", "game-b", 7,
                    io.gameframe.runtime.CrossProcessMigrationCoordinator.Phase.PREPARING, 2,
                    System.currentTimeMillis(), new byte[]{9});
            assertFalse(journal.compareAndSet(id, 1, stale).toCompletableFuture().get());
            assertEquals(1, journal.list(io.gameframe.runtime.CrossProcessMigrationCoordinator.Phase.RECOVERY_REQUIRED, 10)
                    .toCompletableFuture().get().size());
            assertTrue(journal.delete(id, 2).toCompletableFuture().get());
            assertTrue(journal.load(id).toCompletableFuture().get().isEmpty());
        }
    }}
