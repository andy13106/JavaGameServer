package io.gameframe.storage.fault;

import com.mongodb.client.MongoClients;
import io.gameframe.storage.*;
import io.gameframe.storage.mongo.MongoDocumentStore;
import io.gameframe.storage.mysql.MySqlStore;
import io.gameframe.storage.redis.RedisStore;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "GAME_TEST_P35_PROXY", matches = ".+")
class LateResponseRetryMatrixIT {
    private static String required(String name) {
        String value = System.getenv(name);
        assertNotNull(value, "missing " + name);
        assertFalse(value.isBlank(), "blank " + name);
        return value;
    }

    private static int port(String name) {
        return Integer.parseInt(required(name));
    }

    private static ResiliencePolicy policy() {
        return new ResiliencePolicy(2, Duration.ofMillis(750), Duration.ofMillis(25),
            Duration.ofMillis(25), 10, Duration.ofSeconds(2));
    }

    private static void assertRetryStats(ResilientExecutor executor) {
        ResilientExecutor.Stats stats = executor.stats();
        assertEquals(1, stats.calls());
        assertEquals(2, stats.attempts());
        assertEquals(1, stats.retries());
        assertEquals(1, stats.timeouts());
        assertEquals(1, stats.successes());
        assertEquals(0, stats.failures());
    }

    @Test
    void mongoIdempotentRetryWinsAndLateAppliedResponseIsIgnored() throws Exception {
        String targetUri = required("GAME_TEST_P35_PROXY_MONGO_URI");
        String database = "gameframe_p35_retry_" + UUID.randomUUID().toString().replace("-", "");
        StoreKey key = new StoreKey("fault_retry", "mongo_" + UUID.randomUUID());
        WriteCommand command = WriteCommand.create("retry-stall", Map.of("value", "persisted"));
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_MONGO_PORT"));
             MongoDocumentStore stalled = new MongoDocumentStore(
                 "mongodb://127.0.0.1:" + proxy.port(), database, 1, 4);
             MongoDocumentStore retry = new MongoDocumentStore(targetUri, database, 1, 4);
             ResilientExecutor executor = new ResilientExecutor(policy(), 1, 16, "mongo-late-retry")) {
            assertTrue(stalled.load(key).toCompletableFuture().get(15, TimeUnit.SECONDS).isEmpty());
            proxy.armStallNextServerResponse(Duration.ofSeconds(3));
            AtomicInteger attempts = new AtomicInteger();
            AtomicReference<CompletionStage<WriteResult>> first = new AtomicReference<>();
            CompletionStage<WriteResult> result = executor.execute("mongo-write",
                ResilientExecutor.RetryPermission.IDEMPOTENT, () -> {
                    CompletionStage<WriteResult> stage = attempts.incrementAndGet() == 1
                        ? stalled.write(key, command) : retry.write(key, command);
                    if (attempts.get() == 1) first.set(stage);
                    return stage;
                }, ResilientExecutor.AttemptTimeoutException.class::isInstance);

            assertTrue(proxy.awaitResponseStalled(Duration.ofSeconds(10)), "Mongo response was not stalled");
            assertEquals(WriteResult.Status.ALREADY_APPLIED,
                result.toCompletableFuture().get(10, TimeUnit.SECONDS).status());
            assertEquals(WriteResult.Status.APPLIED,
                first.get().toCompletableFuture().get(10, TimeUnit.SECONDS).status());
            assertEquals(WriteResult.Status.ALREADY_APPLIED, result.toCompletableFuture().get().status());
            assertRetryStats(executor);

            try (var client = MongoClients.create(targetUri)) {
                Document doc = client.getDatabase(database).getCollection(key.section())
                    .find(eq("_id", key.id())).first();
                assertNotNull(doc);
                assertEquals(1L, ((Number) doc.get("_version")).longValue());
                assertEquals("persisted", doc.get("data", Document.class).getString("value"));
            }
        } finally {
            try (var client = MongoClients.create(targetUri)) { client.getDatabase(database).drop(); }
        }
    }

    @Test
    void mysqlIdempotentRetryWinsAndLateCommitResponseIsIgnored() throws Exception {
        String targetUrl = required("GAME_TEST_P35_PROXY_MYSQL_URL");
        String password = required("GAME_TEST_P35_PROXY_MYSQL_PASSWORD");
        String table = "retry_" + UUID.randomUUID().toString().replace("-", "");
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_MYSQL_PORT"));
             MySqlStore stalled = new MySqlStore(proxyUrl(proxy), "root", password, 1, 4);
             MySqlStore retry = new MySqlStore(targetUrl, "root", password, 1, 4);
             ResilientExecutor executor = new ResilientExecutor(policy(), 1, 16, "mysql-late-retry")) {
            assertEquals(1, stalled.execute(table, connection -> {
                try (var statement = connection.createStatement()) {
                    statement.executeUpdate("CREATE TABLE " + table
                        + " (id VARCHAR(64) PRIMARY KEY, value VARCHAR(64) NOT NULL)");
                    return 1;
                }
            }).toCompletableFuture().get(15, TimeUnit.SECONDS));

            proxy.armStallNextServerResponse(Duration.ofSeconds(3));
            AtomicInteger attempts = new AtomicInteger();
            AtomicReference<CompletionStage<Integer>> first = new AtomicReference<>();
            CompletionStage<Integer> result = executor.execute("mysql-write",
                ResilientExecutor.RetryPermission.IDEMPOTENT, () -> {
                    MySqlStore store = attempts.incrementAndGet() == 1 ? stalled : retry;
                    CompletionStage<Integer> stage = store.execute(table, connection -> {
                        try (var statement = connection.prepareStatement("INSERT INTO " + table
                            + " (id, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value=VALUES(value)")) {
                            statement.setString(1, "one");
                            statement.setString(2, "persisted");
                            return statement.executeUpdate();
                        }
                    });
                    if (attempts.get() == 1) first.set(stage);
                    return stage;
                }, ResilientExecutor.AttemptTimeoutException.class::isInstance);

            assertTrue(proxy.awaitResponseStalled(Duration.ofSeconds(10)), "MySQL response was not stalled");
            int retryResult = result.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertTrue(retryResult >= 0);
            assertTrue(first.get().toCompletableFuture().get(10, TimeUnit.SECONDS) >= 1);
            assertEquals(retryResult, result.toCompletableFuture().get());
            assertRetryStats(executor);

            try (var connection = DriverManager.getConnection(targetUrl, "root", password);
                 var statement = connection.prepareStatement("SELECT COUNT(*), MAX(value) FROM " + table
                     + " WHERE id=?")) {
                statement.setString(1, "one");
                try (var rows = statement.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals(1, rows.getInt(1));
                    assertEquals("persisted", rows.getString(2));
                }
            }
        } finally {
            try (var connection = DriverManager.getConnection(targetUrl, "root", password);
                 var statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    @Test
    void redisIdempotentRetryWinsAndLateSetResponseIsIgnored() throws Exception {
        String targetUri = required("GAME_TEST_P35_PROXY_REDIS_URI");
        String key = "gameframe:p35:retry:" + UUID.randomUUID();
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_REDIS_PORT"));
             RedisStore stalled = new RedisStore("redis://127.0.0.1:" + proxy.port(), 1, 4, false);
             RedisStore retry = new RedisStore(targetUri, 1, 4);
             ResilientExecutor executor = new ResilientExecutor(policy(), 1, 16, "redis-late-retry")) {
            assertNull(stalled.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
            proxy.armStallNextServerResponse(Duration.ofSeconds(3));
            AtomicInteger attempts = new AtomicInteger();
            AtomicReference<CompletionStage<String>> first = new AtomicReference<>();
            CompletionStage<String> result = executor.execute("redis-set",
                ResilientExecutor.RetryPermission.IDEMPOTENT, () -> {
                    CompletionStage<String> stage = attempts.incrementAndGet() == 1
                        ? stalled.set(key, "persisted", Duration.ofMinutes(1))
                        : retry.set(key, "persisted", Duration.ofMinutes(1));
                    if (attempts.get() == 1) first.set(stage);
                    return stage;
                }, ResilientExecutor.AttemptTimeoutException.class::isInstance);

            assertTrue(proxy.awaitResponseStalled(Duration.ofSeconds(10)), "Redis response was not stalled");
            assertEquals("OK", result.toCompletableFuture().get(10, TimeUnit.SECONDS));
            assertEquals("OK", first.get().toCompletableFuture().get(10, TimeUnit.SECONDS));
            assertEquals("OK", result.toCompletableFuture().get());
            assertRetryStats(executor);
            assertEquals("persisted", retry.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
        } finally {
            try (var direct = new RedisStore(targetUri, 1, 4)) {
                direct.set(key, "", Duration.ofMillis(1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) { }
        }
    }

    private static String proxyUrl(TcpFaultProxy proxy) {
        return "jdbc:mysql://127.0.0.1:" + proxy.port() + "/"
            + required("GAME_TEST_P35_PROXY_MYSQL_DATABASE")
            + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=5000&socketTimeout=10000";
    }
}