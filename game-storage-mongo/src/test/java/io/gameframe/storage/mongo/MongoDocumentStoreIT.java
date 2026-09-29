package io.gameframe.storage.mongo;
import io.gameframe.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_MONGO_URI", matches=".+")
class MongoDocumentStoreIT {
    @Test void actualMongoSupportsCasNestedPatchSnapshotAndReplay() throws Exception {
        String uri = System.getenv("GAME_TEST_MONGO_URI");
        String database = "gameframe_it_" + UUID.randomUUID().toString().replace("-", "");
        try (var store = new MongoDocumentStore(uri, database, 2, 16)) {
            var key = new StoreKey("player_core", "p1");
            assertTrue(store.load(key).toCompletableFuture().get().isEmpty());
            var create = WriteCommand.create("create", Map.of("gold", 100L, "name", "A"));
            assertTrue(store.write(key, create).toCompletableFuture().get().successful());
            assertEquals(WriteResult.Status.ALREADY_APPLIED, store.write(key, create).toCompletableFuture().get().status());
            var patch = WriteCommand.patch(1, "reward", new DocumentPatch(Map.of("quest.progress", 3L), Set.of("name"), Map.of("gold", 20L)));
            assertEquals(2, store.write(key, patch).toCompletableFuture().get().version());
            assertEquals(WriteResult.Status.ALREADY_APPLIED, store.write(key, patch).toCompletableFuture().get().status());
            assertEquals(WriteResult.Status.CONFLICT, store.write(key, WriteCommand.replace(1, "stale", Map.of("gold", 0L))).toCompletableFuture().get().status());
            var snapshot = store.load(key).toCompletableFuture().get().orElseThrow();
            assertEquals(120L, snapshot.data().get("gold")); assertFalse(snapshot.data().containsKey("name"));
            assertEquals(Map.of("progress", 3L), snapshot.data().get("quest"));
            var first = store.write(key, WriteCommand.patch(2, "a", DocumentPatch.set("level", 1L)));
            var other = store.write(key, WriteCommand.patch(2, "b", DocumentPatch.set("level", 2L)));
            assertTrue(first.toCompletableFuture().get().successful());
            assertEquals(WriteResult.Status.CONFLICT, other.toCompletableFuture().get().status());
            assertTrue(store.write(key, WriteCommand.replace(3, "full", Map.of("gold", 200L))).toCompletableFuture().get().successful());
            store.flush(key).toCompletableFuture().get();
            assertEquals(Map.of("gold", 200L), store.load(key).toCompletableFuture().get().orElseThrow().data());
        } finally {
            // Only the unique database created by this test is dropped.
            try (var client = com.mongodb.client.MongoClients.create(uri)) { client.getDatabase(database).drop(); }
        }
    }
}
