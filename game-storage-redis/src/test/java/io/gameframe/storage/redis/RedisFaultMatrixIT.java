package io.gameframe.storage.redis;

import io.gameframe.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_P35_REDIS_URI", matches=".+")
class RedisFaultMatrixIT {
    static String uri() { return System.getenv("GAME_TEST_P35_REDIS_URI"); }
    static String container() { return System.getenv("GAME_TEST_P35_REDIS_CONTAINER"); }
    static void docker(String action) throws Exception {
        if (container() == null || !container().matches("gameframe-p35-[a-z]+-20260920"))
            throw new IllegalArgumentException("unsafe fault container");
        var process = new ProcessBuilder("docker", action, container()).redirectErrorStream(true).start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "docker command timed out");
        byte[] output = process.getInputStream().readAllBytes();
        assertEquals(0, process.exitValue(), action + ": " + new String(output));
    }
    static void awaitRedis() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (var store = new RedisStore(uri(), 1, 4)) {
                if ("OK".equals(store.set("__p35_ping__", "1", Duration.ofSeconds(2)).toCompletableFuture().get(2, TimeUnit.SECONDS))) return;
            } catch (Exception ignored) { Thread.sleep(300); }
        }
        fail("Redis did not recover");
    }
    @Test void stopFailsInFlightOperationAndRestartRecoversNewConnection() throws Exception {
        String key = "gameframe:p35:fault:" + UUID.randomUUID();
        try (var store = new RedisStore(uri(), 1, 1)) {
            assertNull(store.get(key).toCompletableFuture().get(5, TimeUnit.SECONDS));
            docker("stop");
            var failed = store.get(key);
            var error = assertThrows(ExecutionException.class,
                () -> failed.toCompletableFuture().get(12, TimeUnit.SECONDS)).getCause();
            assertInstanceOf(StorageException.class, error);
        } finally {
            docker("start");
            awaitRedis();
            try (var store = new RedisStore(uri(), 1, 4)) {
                assertTrue(store.set(key, "recovered", Duration.ofSeconds(10)).toCompletableFuture().get(5, TimeUnit.SECONDS).equals("OK"));
                assertEquals("recovered", store.get(key).toCompletableFuture().get(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test void restartPreservesCommittedValue() throws Exception {
        String key = "gameframe:p35:restart:" + UUID.randomUUID();
        String value = "durable";
        try (var store = new RedisStore(uri(), 1, 4)) {
            assertEquals("OK", store.set(key, value, Duration.ofMinutes(1)).toCompletableFuture().get(10, TimeUnit.SECONDS));
            docker("stop");
            docker("start");
            awaitRedis();
            try (var recovered = new RedisStore(uri(), 1, 4)) {
                assertEquals(value, recovered.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
            }
        } finally {
            try (var store = new RedisStore(uri(), 1, 4)) {
                store.set(key, "", Duration.ofMillis(1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) { }
        }
    }

    @Test void stopRejectsBoundedQueue() throws Exception {
        String key = "gameframe:p35:queue:" + UUID.randomUUID();
        try (var store = new RedisStore(uri(), 1, 1)) {
            assertNull(store.get(key).toCompletableFuture().get(5, TimeUnit.SECONDS));
            docker("stop");
            var futures = new ArrayList<CompletionStage<String>>();
            for (int i = 0; i < 16; i++) futures.add(store.get(key + ":" + i));
            var outcomes = new ArrayList<StorageException.Outcome>();
            for (var future : futures) {
                try {
                    future.toCompletableFuture().get(12, TimeUnit.SECONDS);
                    fail("operation unexpectedly succeeded while Redis was stopped");
                } catch (ExecutionException error) {
                    outcomes.add(assertInstanceOf(StorageException.class, error.getCause()).outcome());
                }
            }
            assertTrue(outcomes.contains(StorageException.Outcome.NOT_EXECUTED),
                "bounded executor must reject at least one request: " + outcomes);
            assertTrue(outcomes.stream().allMatch(outcome -> outcome == StorageException.Outcome.NOT_EXECUTED
                    || outcome == StorageException.Outcome.UNKNOWN), outcomes::toString);
        } finally {
            docker("start");
            awaitRedis();
        }
    }
}