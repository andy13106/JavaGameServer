package io.gameframe.storage.mongo;

import com.mongodb.client.MongoClients;
import io.gameframe.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_P35_MONGO_URI", matches=".+")
class MongoFaultMatrixIT {
    static String uri() { return System.getenv("GAME_TEST_P35_MONGO_URI"); }
    static String container() { return System.getenv("GAME_TEST_P35_MONGO_CONTAINER"); }
    static void docker(String action) throws Exception {
        if (container() == null || !container().matches("gameframe-p35-[a-z]+-20260920"))
            throw new IllegalArgumentException("unsafe fault container");
        var process = new ProcessBuilder("docker", action, container()).redirectErrorStream(true).start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "docker command timed out");
        byte[] output = process.getInputStream().readAllBytes();
        assertEquals(0, process.exitValue(), action + ": " + new String(output));
    }
    static void awaitMongo() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (var client = MongoClients.create(uri())) {
                client.getDatabase("admin").runCommand(new org.bson.Document("ping", 1));
                return;
            } catch (RuntimeException ignored) { Thread.sleep(300); }
        }
        fail("Mongo did not recover");
    }
    @Test void stopRejectsBoundedQueueAndRestartRecoversNewStore() throws Exception {
        String database = "gameframe_p35_" + UUID.randomUUID().toString().replace("-", "");
        StoreKey key = new StoreKey("fault_component", "p1");
        try (var store = new MongoDocumentStore(uri(), database, 1, 1)) {
            docker("stop");
            var first = store.load(key);
            var second = store.load(key);
            var third = store.load(key);
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
            awaitMongo();
            try (var store = new MongoDocumentStore(uri(), database, 1, 4)) {
                assertTrue(store.load(key).toCompletableFuture().get(15, TimeUnit.SECONDS).isEmpty());
            }
            try (var client = MongoClients.create(uri())) { client.getDatabase(database).drop(); }
        }
    }

    @Test void restartPreservesCommittedDocument() throws Exception {
        String database = "gameframe_p35_restart_" + UUID.randomUUID().toString().replace("-", "");
        StoreKey key = new StoreKey("restart_component", "p1");
        String uri = uri();
        try (var store = new MongoDocumentStore(uri, database, 1, 4)) {
            assertEquals(WriteResult.Status.APPLIED,
                store.write(key, WriteCommand.create("restart", Map.of("value", "durable")))
                    .toCompletableFuture().get(15, TimeUnit.SECONDS).status());
            docker("stop");
            docker("start");
            awaitMongo();
            try (var recovered = new MongoDocumentStore(uri, database, 1, 4)) {
                assertEquals(Optional.of(new Snapshot(1, Map.of("value", "durable"))),
                    recovered.load(key).toCompletableFuture().get(15, TimeUnit.SECONDS));
            }
        } finally {
            try (var client = MongoClients.create(uri)) { client.getDatabase(database).drop(); }
        }
    }
}
