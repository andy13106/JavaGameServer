package io.gameframe.storage.fault;

import com.mongodb.client.MongoClients;
import io.gameframe.storage.*;
import io.gameframe.storage.mongo.MongoDocumentStore;
import io.gameframe.storage.mysql.MySqlStore;
import io.gameframe.storage.redis.RedisStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_P35_MATRIX", matches=".+")
class BacklogMatrixIT {
    private static String required(String name) {
        String value = System.getenv(name);
        assertNotNull(value, "missing " + name);
        assertFalse(value.isBlank(), "blank " + name);
        return value;
    }

    private static int port(String name) {
        return Integer.parseInt(required(name));
    }

    private static List<StoreKey> mongoLaneKeys(int laneCount) {
        Map<Integer, StoreKey> keys = new HashMap<>();
        for (int i = 0; keys.size() < laneCount && i < 10000; i++) {
            StoreKey key = new StoreKey("matrix", "lane-" + i);
            keys.putIfAbsent(Math.floorMod(key.hashCode(), laneCount), key);
        }
        assertEquals(laneCount, keys.size(), "could not find Mongo lane keys");
        return List.copyOf(keys.values());
    }

    private static List<String> stringLaneKeys(int laneCount) {
        Map<Integer, String> keys = new HashMap<>();
        for (int i = 0; keys.size() < laneCount && i < 10000; i++) {
            String key = "gameframe:p35:matrix:lane-" + i;
            keys.putIfAbsent(Math.floorMod(key.hashCode(), laneCount), key);
        }
        assertEquals(laneCount, keys.size(), "could not find string lane keys");
        return List.copyOf(keys.values());
    }
    private static void docker(String action, String envName) throws Exception {
        String container = required(envName);
        assertTrue(container.matches("gameframe-p35-[a-z]+-20260920"), "unsafe matrix container");
        var process = new ProcessBuilder("docker", action, container).redirectErrorStream(true).start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "docker command timed out");
        String output = new String(process.getInputStream().readAllBytes());
        assertEquals(0, process.exitValue(), action + ": " + output);
    }

    private static void awaitMongo() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (var client = MongoClients.create(required("GAME_TEST_P35_MATRIX_MONGO_URI"))) {
                client.getDatabase("admin").runCommand(new org.bson.Document("ping", 1));
                return;
            } catch (RuntimeException ignored) { Thread.sleep(300); }
        }
        fail("Mongo did not recover");
    }

    private static void awaitRedis() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (var store = new RedisStore(required("GAME_TEST_P35_MATRIX_REDIS_URI"), 1, 4)) {
                store.get("__p35_matrix_ping__").toCompletableFuture().get(2, TimeUnit.SECONDS);
                return;
            } catch (Exception ignored) { Thread.sleep(300); }
        }
        fail("Redis did not recover");
    }

    private static void awaitMysql() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (System.nanoTime() < deadline) {
            var process = new ProcessBuilder("docker", "exec", required("GAME_TEST_P35_MATRIX_MYSQL_CONTAINER"),
                "mysqladmin", "ping", "-uroot", "-p" + required("GAME_TEST_P35_MATRIX_MYSQL_PASSWORD"))
                .redirectErrorStream(true).start();
            if (process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0) return;
            Thread.sleep(500);
        }
        fail("MySQL did not recover");
    }

    private static void assertBoundedQueue(List<? extends CompletionStage<?>> operations, int expectedRejected)
        throws Exception {
        int rejected = 0;
        for (CompletionStage<?> operation : operations) {
            try {
                operation.toCompletableFuture().get(15, TimeUnit.SECONDS);
                fail("stopped backend unexpectedly completed an operation");
            } catch (ExecutionException error) {
                Throwable cause = error.getCause();
                if (cause instanceof StorageException storage &&
                    storage.outcome() == StorageException.Outcome.NOT_EXECUTED) rejected++;
            } catch (TimeoutException error) {
                fail("operation exceeded bounded failure window", error);
            }
        }
        assertTrue(rejected >= 1 && rejected <= expectedRejected + 1,
            "queue rejection count must be positive and bounded by capacity plus one scheduling race: " + rejected);
    }

    @Test void mongoCapacityMatrixRejectsOnlyBeyondQueue() throws Exception {
        String uri = required("GAME_TEST_P35_MATRIX_MONGO_URI");
        String database = "gameframe_p35_matrix_" + UUID.randomUUID().toString().replace("-", "");
        List<MongoDocumentStore> stores = new ArrayList<>();
        try {
            for (int capacity : new int[]{1, 2, 4}) {
                var store = new MongoDocumentStore(uri, database + "_" + capacity, 1, capacity);
                stores.add(store);
                assertTrue(store.load(new StoreKey("matrix", "warm")).toCompletableFuture()
                    .get(15, TimeUnit.SECONDS).isEmpty());
            }
            var mongoMultiLane = new MongoDocumentStore(uri, database + "_lanes", 2, 1);
            stores.add(mongoMultiLane);
            for (StoreKey key : mongoLaneKeys(2))
                assertTrue(mongoMultiLane.load(key).toCompletableFuture().get(15, TimeUnit.SECONDS).isEmpty());
            docker("stop", "GAME_TEST_P35_MATRIX_MONGO_CONTAINER");
            for (int capacity : new int[]{1, 2, 4}) {
                var store = stores.get(capacity == 1 ? 0 : capacity == 2 ? 1 : 2);
                List<CompletionStage<?>> operations = new ArrayList<>();
                for (int i = 0; i < capacity + 2; i++)
                    operations.add(store.load(new StoreKey("matrix", "same")));
                assertBoundedQueue(operations, capacity);
            }
            List<CompletionStage<?>> mongoLaneOperations = new ArrayList<>();
            for (StoreKey key : mongoLaneKeys(2))
                for (int i = 0; i < 3; i++) mongoLaneOperations.add(mongoMultiLane.load(key));
            assertBoundedQueue(mongoLaneOperations, 2);        } finally {
            stores.forEach(store -> { try { store.close(); } catch (Exception ignored) { } });
            docker("start", "GAME_TEST_P35_MATRIX_MONGO_CONTAINER");
            awaitMongo();
            try (var client = MongoClients.create(uri)) {
                client.getDatabase(database + "_1").drop();
                client.getDatabase(database + "_2").drop();
                client.getDatabase(database + "_4").drop();
            }
        }
    }

    @Test void redisCapacityMatrixRejectsOnlyBeyondQueue() throws Exception {
        String uri = required("GAME_TEST_P35_MATRIX_REDIS_URI");
        List<RedisStore> stores = new ArrayList<>();
        try {
            for (int capacity : new int[]{1, 2, 4}) {
                var store = new RedisStore(uri, 1, capacity);
                stores.add(store);
                assertNull(store.get("gameframe:p35:matrix:warm").toCompletableFuture()
                    .get(10, TimeUnit.SECONDS));
            }
            var redisMultiLane = new RedisStore(uri, 2, 1);
            stores.add(redisMultiLane);
            for (String key : stringLaneKeys(2))
                assertNull(redisMultiLane.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
            docker("stop", "GAME_TEST_P35_MATRIX_REDIS_CONTAINER");
            for (int capacity : new int[]{1, 2, 4}) {
                var store = stores.get(capacity == 1 ? 0 : capacity == 2 ? 1 : 2);
                List<CompletionStage<?>> operations = new ArrayList<>();
                for (int i = 0; i < capacity + 2; i++)
                    operations.add(store.get("gameframe:p35:matrix:same"));
                assertBoundedQueue(operations, capacity);
            }
            List<CompletionStage<?>> redisLaneOperations = new ArrayList<>();
            for (String key : stringLaneKeys(2))
                for (int i = 0; i < 3; i++) redisLaneOperations.add(redisMultiLane.get(key));
            assertBoundedQueue(redisLaneOperations, 2);        } finally {
            stores.forEach(store -> { try { store.close(); } catch (Exception ignored) { } });
            docker("start", "GAME_TEST_P35_MATRIX_REDIS_CONTAINER");
            awaitRedis();
        }
    }

    @Test void mysqlCapacityMatrixRejectsOnlyBeyondQueue() throws Exception {
        String url = required("GAME_TEST_P35_MATRIX_MYSQL_URL");
        String user = required("GAME_TEST_P35_MATRIX_MYSQL_USER");
        String password = required("GAME_TEST_P35_MATRIX_MYSQL_PASSWORD");
        List<MySqlStore> stores = new ArrayList<>();
        try {
            for (int capacity : new int[]{1, 2, 4}) {
                var store = new MySqlStore(url, user, password, 1, capacity);
                stores.add(store);
                assertEquals(1, store.execute("matrix", c -> 1).toCompletableFuture()
                    .get(10, TimeUnit.SECONDS));
            }
            var mysqlMultiLane = new MySqlStore(url, user, password, 2, 1);
            stores.add(mysqlMultiLane);
            for (String key : stringLaneKeys(2))
                assertEquals(1, mysqlMultiLane.execute(key, c -> 1).toCompletableFuture().get(10, TimeUnit.SECONDS));
            docker("stop", "GAME_TEST_P35_MATRIX_MYSQL_CONTAINER");
            for (int capacity : new int[]{1, 2, 4}) {
                var store = stores.get(capacity == 1 ? 0 : capacity == 2 ? 1 : 2);
                List<CompletionStage<?>> operations = new ArrayList<>();
                for (int i = 0; i < capacity + 2; i++)
                    operations.add(store.execute("matrix", c -> 1));
                assertBoundedQueue(operations, capacity);
            }
            List<CompletionStage<?>> mysqlLaneOperations = new ArrayList<>();
            for (String key : stringLaneKeys(2))
                for (int i = 0; i < 3; i++) mysqlLaneOperations.add(mysqlMultiLane.execute(key, c -> 1));
            assertBoundedQueue(mysqlLaneOperations, 2);        } finally {
            stores.forEach(store -> { try { store.close(); } catch (Exception ignored) { } });
            docker("start", "GAME_TEST_P35_MATRIX_MYSQL_CONTAINER");
            awaitMysql();
        }
    }
}
