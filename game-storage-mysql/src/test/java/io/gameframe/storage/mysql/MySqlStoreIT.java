package io.gameframe.storage.mysql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import static org.junit.jupiter.api.Assertions.*;
@EnabledIfEnvironmentVariable(named="GAME_TEST_MYSQL_URL", matches=".+")
class MySqlStoreIT {
    @Test void committedDataSurvivesAndFailedTransactionRollsBack() throws Exception {
        String table = "gameframe_it_" + UUID.randomUUID().toString().replace("-", "");
        try (var store = new MySqlStore(System.getenv("GAME_TEST_MYSQL_URL"),
                System.getenv("GAME_TEST_MYSQL_USER"), System.getenv("GAME_TEST_MYSQL_PASSWORD"), 2, 32)) {
            try {
                store.execute(table, c -> { try (var s = c.createStatement()) { s.execute("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, gold BIGINT) ENGINE=InnoDB"); } return null; }).toCompletableFuture().get();
                store.transaction(table, c -> { try (var s = c.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) { s.setLong(1, 1); s.setLong(2, 100); s.executeUpdate(); } return null; }).toCompletableFuture().get();
                var failed = store.transaction(table, c -> { try (var s = c.createStatement()) { s.executeUpdate("UPDATE " + table + " SET gold=0"); } throw new IllegalStateException("rollback"); });
                assertThrows(ExecutionException.class, () -> failed.toCompletableFuture().get());
                long gold = store.execute(table, c -> { try (var s = c.createStatement(); var rows = s.executeQuery("SELECT gold FROM " + table + " WHERE id=1")) { rows.next(); return rows.getLong(1); } }).toCompletableFuture().get();
                assertEquals(100, gold);
            } finally {
                store.execute(table, c -> { try (var s = c.createStatement()) { s.execute("DROP TABLE IF EXISTS " + table); } return null; }).toCompletableFuture().get();
            }
        }
    }
}
