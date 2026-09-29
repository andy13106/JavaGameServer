package io.gameframe.storage;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class DocumentStoreTest {
    private final StoreKey key = new StoreKey("player_core", "p1");
    @Test void partialFullAndRepeatedWritesShareVersionAndOrder() throws Exception {
        try (var store = new MemoryDocumentStore(2, 32)) {
            assertEquals(WriteResult.Status.APPLIED, store.write(key, WriteCommand.create("create", Map.of("gold", 100L, "name", "A"))).toCompletableFuture().get().status());
            var patch = WriteCommand.patch(1, "reward-1", new DocumentPatch(Map.of("quest.progress", 3L), Set.of(), Map.of("gold", 20L)));
            assertEquals(2, store.write(key, patch).toCompletableFuture().get().version());
            assertEquals(WriteResult.Status.ALREADY_APPLIED, store.write(key, patch).toCompletableFuture().get().status());
            assertEquals(WriteResult.Status.CONFLICT, store.write(key, WriteCommand.replace(1, "stale", Map.of("gold", 0L))).toCompletableFuture().get().status());
            var snapshot = store.load(key).toCompletableFuture().get().orElseThrow();
            assertEquals(120L, snapshot.data().get("gold"));
            assertEquals(Map.of("progress", 3L), snapshot.data().get("quest"));
            assertEquals("A", snapshot.data().get("name"));
            var full = store.write(key, WriteCommand.replace(2, "full", Map.of("gold", 120L, "name", "B")));
            var next = store.write(key, WriteCommand.patch(3, "next", DocumentPatch.set("level", 2L)));
            assertTrue(full.toCompletableFuture().get().successful()); assertTrue(next.toCompletableFuture().get().successful());
            store.flush(key).toCompletableFuture().get();
            snapshot = store.load(key).toCompletableFuture().get().orElseThrow();
            assertEquals(4, snapshot.version()); assertEquals("B", snapshot.data().get("name")); assertFalse(snapshot.data().containsKey("quest"));
        }
    }
    @Test void submissionDeepCopiesMutableObjectsAndChangedRetryConflicts() throws Exception {
        try (var store = new MemoryDocumentStore(1, 8)) {
            Map<String,Object> nested = new HashMap<>(Map.of("count", 1L)); Map<String,Object> data = new HashMap<>(Map.of("bag", nested));
            var command = WriteCommand.create("c", data); nested.put("count", 99L);
            store.write(key, command).toCompletableFuture().get();
            assertEquals(Map.of("count", 1L), store.load(key).toCompletableFuture().get().orElseThrow().data().get("bag"));
            assertEquals(WriteResult.Status.CONFLICT, store.write(key, WriteCommand.create("c", data)).toCompletableFuture().get().status());
        }
    }
    @Test void loadingMissingDoesNotPreventCreationAndFailedPatchDoesNotPartiallyChangeData() throws Exception {
        try (var store = new MemoryDocumentStore(1, 8)) {
            assertTrue(store.load(key).toCompletableFuture().get().isEmpty());
            store.write(key, WriteCommand.create("c", Map.of("gold", "invalid"))).toCompletableFuture().get();
            var bad = WriteCommand.patch(1, "bad", new DocumentPatch(Map.of("name", "changed"), Set.of(), Map.of("gold", 1L)));
            assertThrows(ExecutionException.class, () -> store.write(key, bad).toCompletableFuture().get());
            var data = store.load(key).toCompletableFuture().get().orElseThrow();
            assertEquals(1, data.version()); assertFalse(data.data().containsKey("name"));
        }
    }
    @Test void pathsMustNotOverlapOrTargetArrays() {
        assertThrows(IllegalArgumentException.class, () -> new DocumentPatch(Map.of("bag", Map.of(), "bag.item", 1L), Set.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new DocumentPatch(Map.of("gold", 1L), Set.of("gold"), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> DocumentPatch.set("items.0", 2L));
    }
}
