package io.gameframe.storage.fault;

import com.mongodb.client.MongoClients;
import io.gameframe.storage.*;
import io.gameframe.storage.mongo.MongoDocumentStore;
import io.gameframe.storage.mysql.MySqlStore;
import io.gameframe.storage.redis.RedisStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "GAME_TEST_P35_PRESSURE", matches = ".+")
class ResourcePressureMatrixIT {
    private static String required(String name) {
        String value = System.getenv(name);
        assertNotNull(value, "missing " + name);
        assertFalse(value.isBlank(), "blank " + name);
        return value;
    }

    private static int port(String name) {
        return Integer.parseInt(required(name));
    }

    private static int awaitBounded(List<? extends CompletionStage<?>> operations) throws Exception {
        int rejected = 0;
        for (CompletionStage<?> operation : operations) {
            try {
                operation.toCompletableFuture().get(12, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                Throwable cause = error.getCause();
                if (cause instanceof StorageException storage
                    && storage.outcome() == StorageException.Outcome.NOT_EXECUTED) {
                    rejected++;
                } else {
                    throw error;
                }
            }
        }
        return rejected;
    }

    @Test
    void dirtyDocumentMemoryWatermarkRejectsWithoutCorruptingState() throws Exception {
        String uri = required("GAME_TEST_P35_PRESSURE_MONGO_URI");
        String database = "gameframe_p35_memory_" + UUID.randomUUID().toString().replace("-", "");
        try (MongoDocumentStore store = new MongoDocumentStore(uri, database, 1, 4);
             DirtyDocumentSet dirty = new DirtyDocumentSet(store, "pressure", "player-1",
                 new DirtyDocumentSet.Limits(2, 8, 32_768, 24_000, 32), () -> {})) {
            assertTrue(dirty.open("core").toCompletableFuture().get(15, TimeUnit.SECONDS).isEmpty());
            dirty.markReplace("core", Map.of("value", "small", "gold", 7L));
            DirtyDocumentSet.LocalView before = dirty.localView("core");

            assertThrows(RejectedExecutionException.class,
                () -> dirty.markReplace("core", Map.of("value", "x".repeat(24_000))));
            assertEquals(before, dirty.localView("core"));
            assertEquals(1, dirty.stats().pendingOperations());
            assertTrue(dirty.stats().bufferedBytes() <= 32_768);

            DirtyDocumentSet.FlushResult flushed = dirty.flush().toCompletableFuture()
                .get(15, TimeUnit.SECONDS);
            assertTrue(flushed.complete());
            assertEquals(Map.of("value", "small", "gold", 7L),
                store.load(dirty.key("core")).toCompletableFuture().get(15, TimeUnit.SECONDS)
                    .orElseThrow().data());
        } finally {
            try (var client = MongoClients.create(uri)) { client.getDatabase(database).drop(); }
        }
    }

    @Test
    void threeBackendsBoundConcurrentBacklogWhileConnectionsAreStalled() throws Exception {
        String mongoUri = required("GAME_TEST_P35_PRESSURE_MONGO_URI");
        String redisUri = required("GAME_TEST_P35_PRESSURE_REDIS_URI");
        String mysqlPassword = required("GAME_TEST_P35_PRESSURE_MYSQL_PASSWORD");
        String database = "gameframe_p35_joint_" + UUID.randomUUID().toString().replace("-", "");
        StoreKey mongoKey = new StoreKey("pressure", "same");
        String redisKey = "gameframe:p35:pressure:" + UUID.randomUUID();

        try (TcpFaultProxy mongoProxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PRESSURE_MONGO_PORT"));
             TcpFaultProxy redisProxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PRESSURE_REDIS_PORT"));
             TcpFaultProxy mysqlProxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PRESSURE_MYSQL_PORT"));
             MongoDocumentStore mongo = new MongoDocumentStore(
                 "mongodb://127.0.0.1:" + mongoProxy.port(), database, 1, 2);
             RedisStore redis = new RedisStore("redis://127.0.0.1:" + redisProxy.port(), 1, 2, false);
             MySqlStore mysql = new MySqlStore(mysqlUrl(mysqlProxy), "root", mysqlPassword, 1, 2)) {

            assertTrue(mongo.load(mongoKey).toCompletableFuture().get(15, TimeUnit.SECONDS).isEmpty());
            assertNull(redis.get(redisKey).toCompletableFuture().get(10, TimeUnit.SECONDS));
            assertEquals(1, mysql.execute("warm", connection -> 1).toCompletableFuture()
                .get(10, TimeUnit.SECONDS));

            mongoProxy.armStallNextServerResponse(Duration.ofSeconds(3));
            redisProxy.armStallNextServerResponse(Duration.ofSeconds(3));
            mysqlProxy.armStallNextServerResponse(Duration.ofSeconds(3));

            List<CompletionStage<?>> mongoOps = new ArrayList<>();
            List<CompletionStage<?>> redisOps = new ArrayList<>();
            List<CompletionStage<?>> mysqlOps = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                mongoOps.add(mongo.load(mongoKey));
                redisOps.add(redis.get(redisKey));
                mysqlOps.add(mysql.execute("same", connection -> {
                    try (var statement = connection.createStatement();
                         var rows = statement.executeQuery("SELECT 1")) {
                        return rows.next() ? rows.getInt(1) : 0;
                    }
                }));
            }

            assertTrue(mongoProxy.awaitResponseStalled(Duration.ofSeconds(10)), "Mongo did not stall");
            assertTrue(redisProxy.awaitResponseStalled(Duration.ofSeconds(10)), "Redis did not stall");
            assertTrue(mysqlProxy.awaitResponseStalled(Duration.ofSeconds(10)), "MySQL did not stall");

            CompletableFuture<Integer> mongoRejected = CompletableFuture.supplyAsync(() -> bounded(mongoOps));
            CompletableFuture<Integer> redisRejected = CompletableFuture.supplyAsync(() -> bounded(redisOps));
            CompletableFuture<Integer> mysqlRejected = CompletableFuture.supplyAsync(() -> bounded(mysqlOps));
            assertBoundedRejections(mongoRejected.get(15, TimeUnit.SECONDS), "Mongo");
            assertBoundedRejections(redisRejected.get(15, TimeUnit.SECONDS), "Redis");
            assertBoundedRejections(mysqlRejected.get(15, TimeUnit.SECONDS), "MySQL");
        } finally {
            try (var client = MongoClients.create(mongoUri)) { client.getDatabase(database).drop(); }
            try (var redis = new RedisStore(redisUri, 1, 2)) {
                redis.set(redisKey, "", Duration.ofMillis(1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) { }
        }
    }

    private static void assertBoundedRejections(int rejected, String backend) {
        assertTrue(rejected >= 1 && rejected <= 3, backend + " rejection count exceeded the bounded scheduling window: " + rejected);
    }

    private static int bounded(List<? extends CompletionStage<?>> operations) {
        try { return awaitBounded(operations); }
        catch (Exception error) { throw new CompletionException(error); }
    }

    private static String mysqlUrl(TcpFaultProxy proxy) {
        return "jdbc:mysql://127.0.0.1:" + proxy.port() + "/"
            + required("GAME_TEST_P35_PRESSURE_MYSQL_DATABASE")
            + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=5000&socketTimeout=10000";
    }
}