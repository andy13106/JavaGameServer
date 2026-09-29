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
import java.util.*;
import java.util.concurrent.*;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_P35_PROXY", matches=".+")
class ProtocolFaultMatrixIT {
    private static String required(String name) {
        String value = System.getenv(name);
        assumeTrue(value != null && !value.isBlank(), "missing " + name);
        return value;
    }

    private static void assumeTrue(boolean condition, String message) {
        if (!condition) throw new org.opentest4j.TestAbortedException(message);
    }

    private static int port(String name) {
        return Integer.parseInt(required(name));
    }

    private static Throwable failure(CompletionStage<?> stage, long seconds) throws Exception {
        ExecutionException error = assertThrows(ExecutionException.class,
            () -> stage.toCompletableFuture().get(seconds, TimeUnit.SECONDS));
        return error.getCause() == null ? error : error.getCause();
    }

    @Test void mongoWriteAckLossIsUnknownButDocumentIsDurable() throws Exception {
        String targetUri = required("GAME_TEST_P35_PROXY_MONGO_URI");
        String database = "gameframe_p35_proxy_" + UUID.randomUUID().toString().replace("-", "");
        StoreKey key = new StoreKey("fault_ack", "mongo_" + UUID.randomUUID());
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_MONGO_PORT"));
             MongoDocumentStore store = new MongoDocumentStore("mongodb://127.0.0.1:" + proxy.port(), database, 1, 4)) {
            assertTrue(store.load(key).toCompletableFuture().get(15, TimeUnit.SECONDS).isEmpty());
            proxy.armDropNextServerResponse();
            CompletionStage<WriteResult> write = store.write(key, WriteCommand.create("ack-loss", Map.of("value", "persisted")));
            assertTrue(proxy.awaitResponseDropped(Duration.ofSeconds(10)), "proxy did not drop Mongo response");
            try {
                WriteResult result = write.toCompletableFuture().get(20, TimeUnit.SECONDS);
                assertTrue(result.successful(), "reconciled Mongo write must be confirmed");
            } catch (ExecutionException error) {
                assertInstanceOf(StorageException.class, error.getCause());
                assertEquals(StorageException.Outcome.UNKNOWN,
                    ((StorageException) error.getCause()).outcome());
            }
            try (var client = MongoClients.create(targetUri)) {
                Document document = client.getDatabase(database).getCollection(key.section()).find(eq("_id", key.id())).first();
                assertNotNull(document, "server accepted the write before response loss");
                assertEquals(1L, ((Number) document.get("_version")).longValue());
            }
            try (MongoDocumentStore recovered = new MongoDocumentStore("mongodb://127.0.0.1:" + proxy.port(), database, 1, 4)) {
                assertEquals(Optional.of(new Snapshot(1, Map.of("value", "persisted"))),
                    recovered.load(key).toCompletableFuture().get(15, TimeUnit.SECONDS));
            }
        } finally {
            try (var client = MongoClients.create(targetUri)) { client.getDatabase(database).drop(); }
        }
    }

    @Test void mysqlWriteAckLossIsUnknownButRowIsDurable() throws Exception {
        String targetUrl = required("GAME_TEST_P35_PROXY_MYSQL_URL");
        String table = "ack_" + UUID.randomUUID().toString().replace("-", "");
        String proxyUrl;
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_MYSQL_PORT"));
             MySqlStore store = new MySqlStore(proxyUrl(proxy), "root", required("GAME_TEST_P35_PROXY_MYSQL_PASSWORD"), 1, 4)) {
            proxyUrl = proxyUrl(proxy);
            assertEquals(1, store.execute(table, c -> {
                try (var s = c.createStatement()) {
                    s.executeUpdate("CREATE TABLE " + table + " (id VARCHAR(64) PRIMARY KEY, value VARCHAR(64) NOT NULL)");
                    return 1;
                }
            }).toCompletableFuture().get(15, TimeUnit.SECONDS));
            proxy.armDropNextServerResponse();
            CompletionStage<Integer> write = store.execute(table, c -> {
                try (var s = c.prepareStatement("INSERT INTO " + table + " (id, value) VALUES (?, ?)")) {
                    s.setString(1, "one"); s.setString(2, "persisted"); return s.executeUpdate();
                }
            });
            assertTrue(proxy.awaitResponseDropped(Duration.ofSeconds(10)), "proxy did not drop MySQL response");
            Throwable error = failure(write, 20);
            assertInstanceOf(StorageException.class, error);
            assertEquals(StorageException.Outcome.UNKNOWN, ((StorageException) error).outcome());
            try (var connection = DriverManager.getConnection(targetUrl, "root", required("GAME_TEST_P35_PROXY_MYSQL_PASSWORD"));
                 var s = connection.prepareStatement("SELECT value FROM " + table + " WHERE id=?")) {
                s.setString(1, "one");
                try (var rs = s.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("persisted", rs.getString(1));
                }
            }
            proxyUrl = proxyUrl(proxy);
            try (MySqlStore recovered = new MySqlStore(proxyUrl, "root", required("GAME_TEST_P35_PROXY_MYSQL_PASSWORD"), 1, 4)) {
                assertEquals("persisted", recovered.execute(table, c -> {
                    try (var s = c.prepareStatement("SELECT value FROM " + table + " WHERE id=?")) {
                        s.setString(1, "one");
                        try (var rs = s.executeQuery()) { return rs.next() ? rs.getString(1) : null; }
                    }
                }).toCompletableFuture().get(15, TimeUnit.SECONDS));
            }
        } finally {
            try (var connection = DriverManager.getConnection(targetUrl, "root", required("GAME_TEST_P35_PROXY_MYSQL_PASSWORD"));
                 var s = connection.createStatement()) { s.executeUpdate("DROP TABLE IF EXISTS " + table); }
        }
    }


    @Test void mongoServerStallCausesTimeoutButWriteIsDurable() throws Exception {
        String targetUri = required("GAME_TEST_P35_PROXY_MONGO_URI");
        String database = "gameframe_p35_stall_" + UUID.randomUUID().toString().replace("-", "");
        StoreKey key = new StoreKey("fault_stall", "mongo_" + UUID.randomUUID());
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_MONGO_PORT"));
             MongoDocumentStore store = new MongoDocumentStore("mongodb://127.0.0.1:" + proxy.port(), database, 1, 4)) {
            assertTrue(store.load(key).toCompletableFuture().get(15, TimeUnit.SECONDS).isEmpty());
            proxy.armStallNextServerResponse(Duration.ofSeconds(12));
            CompletionStage<WriteResult> write = store.write(key, WriteCommand.create("stall", Map.of("value", "persisted")));
            assertTrue(proxy.awaitResponseStalled(Duration.ofSeconds(10)), "proxy did not stall Mongo response");
            try {
                WriteResult result = write.toCompletableFuture().get(20, TimeUnit.SECONDS);
                // A retry can reconcile the committed create by its operation ID.
                assertTrue(result.successful(), "reconciled Mongo write must be confirmed");
            } catch (ExecutionException error) {
                assertInstanceOf(StorageException.class, error.getCause());
                assertEquals(StorageException.Outcome.UNKNOWN,
                    ((StorageException) error.getCause()).outcome());
            }
            try (var client = MongoClients.create(targetUri)) {
                Document document = client.getDatabase(database).getCollection(key.section())
                    .find(eq("_id", key.id())).first();
                assertNotNull(document);
                assertEquals(1L, ((Number) document.get("_version")).longValue());
            }
        } finally {
            try (var client = MongoClients.create(targetUri)) { client.getDatabase(database).drop(); }
        }
    }

    @Test void mysqlServerStallCausesTimeoutButWriteIsDurable() throws Exception {
        String targetUrl = required("GAME_TEST_P35_PROXY_MYSQL_URL");
        String table = "stall_" + UUID.randomUUID().toString().replace("-", "");
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_MYSQL_PORT"));
             MySqlStore store = new MySqlStore(proxyUrl(proxy), "root", required("GAME_TEST_P35_PROXY_MYSQL_PASSWORD"), 1, 4)) {
            assertEquals(1, store.execute(table, c -> {
                try (var s = c.createStatement()) {
                    s.executeUpdate("CREATE TABLE " + table + " (id VARCHAR(64) PRIMARY KEY, value VARCHAR(64) NOT NULL)");
                    return 1;
                }
            }).toCompletableFuture().get(15, TimeUnit.SECONDS));
            proxy.armStallNextServerResponse(Duration.ofSeconds(12));
            CompletionStage<Integer> write = store.execute(table, c -> {
                try (var s = c.prepareStatement("INSERT INTO " + table + " (id, value) VALUES (?, ?)")) {
                    s.setString(1, "one"); s.setString(2, "persisted"); return s.executeUpdate();
                }
            });
            assertTrue(proxy.awaitResponseStalled(Duration.ofSeconds(10)), "proxy did not stall MySQL response");
            Throwable error = failure(write, 20);
            assertInstanceOf(StorageException.class, error);
            assertEquals(StorageException.Outcome.UNKNOWN, ((StorageException) error).outcome());
            try (var connection = DriverManager.getConnection(targetUrl, "root", required("GAME_TEST_P35_PROXY_MYSQL_PASSWORD"));
                 var s = connection.prepareStatement("SELECT value FROM " + table + " WHERE id=?")) {
                s.setString(1, "one");
                try (var rs = s.executeQuery()) { assertTrue(rs.next()); }
            }
        } finally {
            try (var connection = DriverManager.getConnection(targetUrl, "root", required("GAME_TEST_P35_PROXY_MYSQL_PASSWORD"));
                 var s = connection.createStatement()) { s.executeUpdate("DROP TABLE IF EXISTS " + table); }
        }
    }

    @Test void redisServerStallCausesTimeoutButWriteIsDurable() throws Exception {
        String targetUri = required("GAME_TEST_P35_PROXY_REDIS_URI");
        String key = "gameframe:p35:stall:" + UUID.randomUUID();
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_REDIS_PORT"));
             RedisStore store = new RedisStore("redis://127.0.0.1:" + proxy.port(), 1, 4, false)) {
            assertNull(store.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
            proxy.armStallNextServerResponse(Duration.ofSeconds(7));
            CompletionStage<String> write = store.set(key, "persisted", Duration.ofMinutes(1));
            assertTrue(proxy.awaitResponseStalled(Duration.ofSeconds(10)), "proxy did not stall Redis response");
            Throwable error = failure(write, 12);
            assertInstanceOf(StorageException.class, error);
            assertEquals(StorageException.Outcome.UNKNOWN, ((StorageException) error).outcome());
            try (RedisStore direct = new RedisStore(targetUri, 1, 4)) {
                assertEquals("persisted", direct.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
            }
        } finally {
            try (RedisStore direct = new RedisStore(targetUri, 1, 4)) {
                direct.set(key, "", Duration.ofMillis(1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) { }
        }
    }

    @Test void redisWriteAckLossIsUnknownButValueIsDurable() throws Exception {
        String targetUri = required("GAME_TEST_P35_PROXY_REDIS_URI");
        String key = "gameframe:p35:proxy:" + UUID.randomUUID();
        String proxyUri;
        try (TcpFaultProxy proxy = new TcpFaultProxy("127.0.0.1", port("GAME_TEST_P35_PROXY_REDIS_PORT"));
             RedisStore store = new RedisStore("redis://127.0.0.1:" + proxy.port(), 1, 4, false)) {
            assertNull(store.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
            proxy.armDropNextServerResponse();
            CompletionStage<String> write = store.set(key, "persisted", Duration.ofMinutes(1));
            assertTrue(proxy.awaitResponseDropped(Duration.ofSeconds(10)), "proxy did not drop Redis response");
            Throwable error = failure(write, 20);
            assertInstanceOf(StorageException.class, error);
            assertEquals(StorageException.Outcome.UNKNOWN, ((StorageException) error).outcome());
            try (RedisStore direct = new RedisStore(targetUri, 1, 4)) {
                assertEquals("persisted", direct.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
            }
            try (RedisStore recovered = new RedisStore("redis://127.0.0.1:" + proxy.port(), 1, 4)) {
                assertEquals("persisted", recovered.get(key).toCompletableFuture().get(10, TimeUnit.SECONDS));
            }
        } finally {
            try (RedisStore direct = new RedisStore(targetUri, 1, 4)) {
                direct.set(key, "", Duration.ofMillis(1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) { }
        }
    }

    private static String proxyUrl(TcpFaultProxy proxy) {
        return "jdbc:mysql://127.0.0.1:" + proxy.port() + "/" + required("GAME_TEST_P35_PROXY_MYSQL_DATABASE") + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=5000&socketTimeout=10000";
    }
}
