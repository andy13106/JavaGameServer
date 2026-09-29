package io.gameframe.storage.mysql;

import io.gameframe.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_P35_MYSQL_URL", matches=".+")
class MySqlFaultMatrixIT {
    static String container() { return System.getenv("GAME_TEST_P35_MYSQL_CONTAINER"); }
    static void docker(String action) throws Exception {
        if (container() == null || !container().matches("gameframe-p35-[a-z]+-20260920"))
            throw new IllegalArgumentException("unsafe fault container");
        var process = new ProcessBuilder("docker", action, container()).redirectErrorStream(true).start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "docker command timed out");
        byte[] output = process.getInputStream().readAllBytes();
        assertEquals(0, process.exitValue(), action + ": " + new String(output));
    }
    static void awaitMysql() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < deadline) {
            var process = new ProcessBuilder("docker", "exec", container(),
                "mysqladmin", "ping", "-uroot", "-pp35root").redirectErrorStream(true).start();
            if (process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0) return;
            Thread.sleep(500);
        }
        fail("MySQL did not recover");
    }
    @Test void stopRejectsBoundedQueueAndRestartRecoversNewConnection() throws Exception {
        String table = "p35_" + UUID.randomUUID().toString().replace("-", "");
        String url = System.getenv("GAME_TEST_P35_MYSQL_URL");
        try (var store = new MySqlStore(url, "root", "p35root", 1, 1)) {
            assertEquals(1, store.execute(table, c -> 1).toCompletableFuture().get(10, TimeUnit.SECONDS));
            docker("stop");
            var first = store.execute(table, c -> 1);
            var second = store.execute(table, c -> 1);
            var third = store.execute(table, c -> 1);
            var thirdError = assertThrows(ExecutionException.class,
                () -> third.toCompletableFuture().get(15, TimeUnit.SECONDS)).getCause();
            assertEquals(StorageException.Outcome.NOT_EXECUTED,
                assertInstanceOf(StorageException.class, thirdError).outcome());
            assertThrows(ExecutionException.class,
                () -> first.toCompletableFuture().get(15, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,
                () -> second.toCompletableFuture().get(15, TimeUnit.SECONDS));
        } finally {
            docker("start");
            awaitMysql();
            try (var store = new MySqlStore(url, "root", "p35root", 1, 4)) {
                assertEquals(1, store.execute(table, c -> 1).toCompletableFuture().get(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test void restartPreservesCommittedRow() throws Exception {
        String table = "restart_" + UUID.randomUUID().toString().replace("-", "");
        String url = System.getenv("GAME_TEST_P35_MYSQL_URL");
        try (var store = new MySqlStore(url, "root", "p35root", 1, 4)) {
            assertEquals(1, store.execute(table, c -> {
                try (var s = c.createStatement()) {
                    s.executeUpdate("CREATE TABLE " + table + " (id VARCHAR(64) PRIMARY KEY, value VARCHAR(64) NOT NULL)");
                    try (var insert = c.prepareStatement("INSERT INTO " + table + " (id, value) VALUES (?, ?)")) {
                        insert.setString(1, "one"); insert.setString(2, "durable"); insert.executeUpdate();
                    }
                    return 1;
                }
            }).toCompletableFuture().get(15, TimeUnit.SECONDS));
            docker("stop");
            docker("start");
            awaitMysql();
            try (var recovered = new MySqlStore(url, "root", "p35root", 1, 4)) {
                assertEquals("durable", recovered.execute(table, c -> {
                    try (var s = c.prepareStatement("SELECT value FROM " + table + " WHERE id=?")) {
                        s.setString(1, "one");
                        try (var rs = s.executeQuery()) { return rs.next() ? rs.getString(1) : null; }
                    }
                }).toCompletableFuture().get(15, TimeUnit.SECONDS));
            }
        } finally {
            try (var connection = java.sql.DriverManager.getConnection(url, "root", "p35root");
                 var s = connection.createStatement()) { s.executeUpdate("DROP TABLE IF EXISTS " + table); }
        }
    }
}