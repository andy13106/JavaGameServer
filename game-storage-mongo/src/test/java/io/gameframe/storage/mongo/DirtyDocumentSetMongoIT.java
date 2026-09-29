package io.gameframe.storage.mongo;
import io.gameframe.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="GAME_TEST_MONGO_URI", matches=".+")
class DirtyDocumentSetMongoIT {
    static <T> T get(CompletionStage<T> stage) throws Exception { return stage.toCompletableFuture().get(15, TimeUnit.SECONDS); }
    @Test void coalescedComponentsFullSnapshotAndLostAckRetryUseActualMongo() throws Exception {
        String uri = System.getenv("GAME_TEST_MONGO_URI");
        String database = "gameframe_dirty_it_" + UUID.randomUUID().toString().replace("-", "");
        try (var mongo = new MongoDocumentStore(uri, database, 2, 32)) {
            var loseAck = new AtomicInteger();
            DocumentStore store = new DocumentStore() {
                public CompletionStage<Optional<Snapshot>> load(StoreKey key) { return mongo.load(key); }
                public CompletionStage<WriteResult> write(StoreKey key, WriteCommand command) {
                    return mongo.write(key, command).thenCompose(result -> loseAck.getAndUpdate(n -> Math.max(0,n-1)) > 0 ?
                        CompletableFuture.failedFuture(new StorageException(StorageException.Outcome.UNKNOWN, "injected lost acknowledgement", null)) :
                        CompletableFuture.completedFuture(result));
                }
                public CompletionStage<Void> flush(StoreKey key) { return mongo.flush(key); }
                public void close() {}
            };
            var dirty = new DirtyDocumentSet(store, "player_component", "p1", 4, 32);
            get(dirty.open("core")); get(dirty.open("bag"));
            dirty.markReplace("core", Map.of("gold", 100L, "level", 1L)); dirty.markReplace("bag", Map.of("slots", 2L));
            assertTrue(get(dirty.flush()).complete());
            for (int i=0; i<100; i++) dirty.markPatch("core", new DocumentPatch(Map.of(), Set.of(), Map.of("gold",1L)));
            dirty.markPatch("core", DocumentPatch.set("quest.progress",3L));
            dirty.markPatch("bag", DocumentPatch.set("slots",4L));
            assertEquals(2, dirty.pendingOperations()); assertTrue(get(dirty.flush()).complete());
            assertEquals(2, get(mongo.load(dirty.key("core"))).orElseThrow().version());
            assertEquals(dirty.localView("core").data().orElseThrow(), get(mongo.load(dirty.key("core"))).orElseThrow().data());
            dirty.markReplace("core", Map.of("gold",200L));
            assertTrue(get(dirty.flush()).complete());
            loseAck.set(1);
            dirty.markPatch("core", new DocumentPatch(Map.of(), Set.of(), Map.of("gold",7L)));
            assertFalse(get(dirty.flush()).complete());
            WriteCommand pinned = dirty.failure("core").orElseThrow().command();
            assertEquals(207L, get(mongo.load(dirty.key("core"))).orElseThrow().data().get("gold"));
            assertFalse(get(dirty.flush()).complete());
            dirty.retryFailed("core"); var result = get(dirty.flushAndClose());
            assertTrue(result.complete()); assertEquals(WriteResult.Status.ALREADY_APPLIED, result.items().getFirst().status());
            assertEquals(3, pinned.expectedVersion());
            assertEquals(Map.of("gold",207L), get(mongo.load(dirty.key("core"))).orElseThrow().data());
            assertEquals(Map.of("slots",4L), get(mongo.load(dirty.key("bag"))).orElseThrow().data());
        } finally {
            try (var client = com.mongodb.client.MongoClients.create(uri)) { client.getDatabase(database).drop(); }
        }
    }
}
