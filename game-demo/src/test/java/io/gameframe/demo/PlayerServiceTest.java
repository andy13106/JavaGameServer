package io.gameframe.demo;
import io.gameframe.runtime.ActorSystem;
import io.gameframe.storage.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class PlayerServiceTest {
    @Test void queuedCommandsAdvanceVersionsOnlyAfterAcknowledgement() throws Exception {
        try (var system = new ActorSystem(2, 1, 128, 4); var store = new MemoryDocumentStore(1, 32)) {
            var player = new PlayerService(system, store, "p"); player.open().toCompletableFuture().get(3, TimeUnit.SECONDS);
            var futures = new ArrayList<CompletableFuture<WriteResult>>();
            for (int i = 0; i < 30; i++) futures.add(player.patch("reward-" + i, new DocumentPatch(Map.of(), Set.of(), Map.of("gold", 1L))).toCompletableFuture());
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(3, TimeUnit.SECONDS);
            var snapshot = player.read().toCompletableFuture().get().orElseThrow();
            assertEquals(31, snapshot.version()); assertEquals(130L, snapshot.data().get("gold"));
        }
    }
    @Test void unknownWriteOutcomeFailsQueuedCommandsWithoutRetrying() throws Exception {
        try (var system = new ActorSystem(2, 1, 32, 4); var memory = new MemoryDocumentStore(1, 32)) {
            var pending = new CompletableFuture<WriteResult>();
            DocumentStore store = new DocumentStore() {
                public CompletionStage<Optional<Snapshot>> load(StoreKey k) { return memory.load(k); }
                public CompletionStage<WriteResult> write(StoreKey k, WriteCommand c) {
                    return c.kind() == WriteCommand.Kind.CREATE ? memory.write(k, c) : pending;
                }
                public CompletionStage<Void> flush(StoreKey k) { return memory.flush(k); }
                public void close() {}
            };
            var player = new PlayerService(system, store, "p"); player.open().toCompletableFuture().get(3, TimeUnit.SECONDS);
            var a = player.patch("a", DocumentPatch.set("level", 2L));
            var b = player.patch("b", DocumentPatch.set("level", 3L));
            pending.completeExceptionally(new StorageException(StorageException.Outcome.UNKNOWN, "lost acknowledgement", null));
            assertThrows(ExecutionException.class, () -> a.toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> b.toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertEquals(1, player.read().toCompletableFuture().get().orElseThrow().version());
        }
    }
    @Test void oldBusinessSnapshotCannotOverwriteNewPatch() throws Exception {
        try (var system = new ActorSystem(2, 1, 32, 4); var store = new MemoryDocumentStore(1, 32)) {
            var player = new PlayerService(system, store, "p");
            var old = player.open().toCompletableFuture().get(3, TimeUnit.SECONDS);
            player.patch("new", new DocumentPatch(Map.of(), Set.of(), Map.of("gold", 20L))).toCompletableFuture().get();
            assertThrows(ExecutionException.class, () -> player.replace("stale", old).toCompletableFuture().get(3, TimeUnit.SECONDS));
            var actual = player.read().toCompletableFuture().get().orElseThrow();
            assertEquals(120L, actual.data().get("gold")); assertEquals(2, actual.version());
        }
    }

}
