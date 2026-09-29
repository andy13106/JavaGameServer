package io.gameframe.storage;

import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BiFunction;
import static org.junit.jupiter.api.Assertions.*;

class DirtyDocumentSetTest {
    static <T> T get(CompletionStage<T> stage) throws Exception { return stage.toCompletableFuture().get(4, TimeUnit.SECONDS); }
    static DocumentPatch inc(String field, long amount) { return new DocumentPatch(Map.of(), Set.of(), Map.of(field, amount)); }
    static final class Store implements DocumentStore {
        final MemoryDocumentStore real = new MemoryDocumentStore(2, 64);
        final List<WriteCommand> writes = new CopyOnWriteArrayList<>();
        volatile BiFunction<StoreKey, WriteCommand, CompletionStage<WriteResult>> behavior;
        public CompletionStage<Optional<Snapshot>> load(StoreKey key) { return real.load(key); }
        public CompletionStage<WriteResult> write(StoreKey key, WriteCommand command) {
            writes.add(command);
            return behavior == null ? real.write(key, command) : behavior.apply(key, command);
        }
        public CompletionStage<Void> flush(StoreKey key) { return real.flush(key); }
        public void close() { real.close(); }
    }
    static DirtyDocumentSet created(Store store, int capacity) throws Exception {
        var dirty = new DirtyDocumentSet(store, "player_component", "p", 4, capacity);
        get(dirty.open("core")); dirty.markReplace("core", Map.of("gold", 100L, "level", 1L));
        assertTrue(get(dirty.flush()).complete()); store.writes.clear(); return dirty;
    }
    @Test void componentsAreIndependentAndDisjointPatchesCoalesce() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8);
            get(dirty.open("bag")); dirty.markReplace("bag", Map.of("slots", 10L));
            dirty.markPatch("core", DocumentPatch.set("quest.progress", 2L));
            dirty.markPatch("core", DocumentPatch.set("level", 2L));
            assertEquals(2, dirty.pendingOperations());
            assertTrue(get(dirty.flush()).complete());
            dirty.markPatch("bag", inc("slots", 3));
            assertTrue(get(dirty.flushAndClose()).complete());
            assertEquals(2L, dirty.current("core").orElseThrow().data().get("level"));
            assertEquals(13L, dirty.current("bag").orElseThrow().data().get("slots"));
            assertNotEquals(dirty.key("core"), dirty.key("bag"));
        }
    }
    @Test void repeatedSetsCoalesceWithoutConsumingMoreSlotsAndFullSnapshotSupersedesUnsealedEdits() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 1);
            for (int i = 0; i < 100; i++) dirty.markPatch("core", DocumentPatch.set("level", (long)i));
            assertEquals(1, dirty.pendingOperations());
            assertEquals(99L, dirty.localView("core").data().orElseThrow().get("level"));
            dirty.markReplace("core", Map.of("gold", 9L));
            assertTrue(get(dirty.flushAndClose()).complete());
            assertEquals(1, store.writes.size());
            assertEquals(WriteCommand.Kind.REPLACE, store.writes.getFirst().kind());
            assertEquals(Map.of("gold", 9L), dirty.current("core").orElseThrow().data());
            assertThrows(RejectedExecutionException.class, () -> dirty.markPatch("core", DocumentPatch.set("x", 1L)));
        }
    }
    @Test void parentChildAndIncrementChangesKeepOrder() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8);
            dirty.markPatch("core", DocumentPatch.set("quest", Map.of("progress", 1L)));
            dirty.markPatch("core", DocumentPatch.set("quest.progress", 5L));
            dirty.markPatch("core", inc("gold", 3));
            dirty.markPatch("core", inc("gold", -2));
            assertTrue(get(dirty.flushAndClose()).complete());
            assertEquals(101L, dirty.current("core").orElseThrow().data().get("gold"));
            assertEquals(Map.of("progress", 5L), dirty.current("core").orElseThrow().data().get("quest"));
            assertEquals(2, store.writes.size());
        }
    }
    @Test void inFlightBatchCannotBeReplacedAndLaterEditsRequireNextFlush() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8);
            var gate = new CompletableFuture<WriteResult>();
            store.behavior = (key, command) -> gate;
            dirty.markPatch("core", inc("gold", 1));
            var first = dirty.flush(); WriteCommand captured = store.writes.getFirst();
            dirty.markReplace("core", Map.of("gold", 150L));
            dirty.markPatch("core", inc("gold", 2));
            var shared = dirty.flush();
            assertEquals(1, store.writes.size()); assertEquals(3, dirty.pendingOperations());
            gate.complete(get(store.real.write(dirty.key("core"), captured)));
            assertTrue(get(first).complete()); assertTrue(get(shared).complete());
            assertEquals(2, dirty.pendingOperations());
            assertEquals(101L, dirty.current("core").orElseThrow().data().get("gold"));
            store.behavior = null;
            assertTrue(get(dirty.flushAndClose()).complete());
            assertEquals(152L, get(store.load(dirty.key("core"))).orElseThrow().data().get("gold"));
            assertEquals(List.of(1L, 2L, 3L), store.writes.stream().map(WriteCommand::expectedVersion).toList());
        }
    }
    @Test void shutdownFreezesIngressAndDrainsEditsBehindTheActiveBoundary() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8);
            var gate = new CompletableFuture<WriteResult>();
            store.behavior = (key, command) -> gate;
            dirty.markPatch("core", inc("gold", 1)); var first = dirty.flush();
            dirty.markPatch("core", inc("gold", 5)); var close = dirty.flushAndClose();
            assertFalse(close.toCompletableFuture().isDone());
            assertThrows(RejectedExecutionException.class, () -> dirty.markReplace("core", Map.of()));
            assertThrows(IllegalStateException.class, dirty::close);
            WriteCommand captured = store.writes.getFirst(); store.behavior = null;
            gate.complete(get(store.real.write(dirty.key("core"), captured)));
            assertTrue(get(first).complete()); var result = get(close);
            assertTrue(result.complete()); assertEquals(2, result.items().getFirst().applied());
            assertEquals(106L, get(store.load(dirty.key("core"))).orElseThrow().data().get("gold"));
            assertTrue(dirty.stats().closed()); assertEquals(0, dirty.pendingOperations()); dirty.close();
        }
    }
    @Test void partialSuccessKeepsConfirmedVersionAndRetriesExactRejectedCommand() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8); var count = new AtomicInteger();
            store.behavior = (key, command) -> count.incrementAndGet() == 2 ?
                CompletableFuture.failedFuture(new StorageException(StorageException.Outcome.NOT_EXECUTED, "queue full", null)) :
                store.real.write(key, command);
            dirty.markPatch("core", DocumentPatch.set("quest", Map.of("done", 1L))); dirty.markPatch("core", DocumentPatch.set("quest.done", 2L));
            var failed = get(dirty.flush());
            assertFalse(failed.complete()); assertEquals(1, failed.items().getFirst().applied());
            assertEquals(2, dirty.current("core").orElseThrow().version());
            assertEquals(Map.of("done", 1L), dirty.current("core").orElseThrow().data().get("quest"));
            assertEquals(1, dirty.pendingOperations());
            WriteCommand original = store.writes.getLast();
            assertFalse(get(dirty.flush()).complete()); assertEquals(2, store.writes.size());
            assertThrows(IllegalStateException.class, () -> dirty.markReplace("core", Map.of("gold", 0L)));
            dirty.retryFailed("core"); store.behavior = null;
            assertTrue(get(dirty.flushAndClose()).complete());
            assertSame(original, store.writes.getLast());
            assertEquals(Map.of("done", 2L), get(store.load(dirty.key("core"))).orElseThrow().data().get("quest"));
        }
    }
    @Test void lostAcknowledgementPausesAndExactRetryDoesNotRepeatIncrement() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8);
            store.behavior = (key, command) -> store.real.write(key, command).thenCompose(v ->
                CompletableFuture.failedFuture(new StorageException(StorageException.Outcome.UNKNOWN, "lost ack", null)));
            dirty.markPatch("core", inc("gold", 7));
            assertFalse(get(dirty.flush()).complete());
            assertEquals(107L, get(store.load(dirty.key("core"))).orElseThrow().data().get("gold"));
            assertEquals(100L, dirty.current("core").orElseThrow().data().get("gold"));
            assertEquals(DirtyDocumentSet.FailureKind.UNKNOWN, dirty.failure("core").orElseThrow().kind());
            WriteCommand original = store.writes.getFirst();
            assertFalse(get(dirty.flush()).complete()); assertEquals(1, store.writes.size());
            dirty.retryFailed("core"); store.behavior = null;
            var result = get(dirty.flushAndClose());
            assertTrue(result.complete()); assertSame(original, store.writes.getLast());
            assertEquals(WriteResult.Status.ALREADY_APPLIED, result.items().getFirst().status());
            assertEquals(107L, dirty.current("core").orElseThrow().data().get("gold"));
        }
    }
    @Test void conflictIsIsolatedAndCannotBeRetriedWithANewVersion() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8); get(dirty.open("bag"));
            get(store.real.write(dirty.key("core"), WriteCommand.patch(1, "other-owner", inc("gold", 50))));
            dirty.markPatch("core", inc("gold", 3)); dirty.markReplace("bag", Map.of("slots", 9L));
            var report = get(dirty.flush());
            assertFalse(report.complete()); assertEquals(1, report.successfulComponents());
            assertEquals(DirtyDocumentSet.FailureKind.CONFLICT, dirty.failure("core").orElseThrow().kind());
            assertEquals(1, dirty.pendingOperations()); assertEquals(9L, dirty.current("bag").orElseThrow().data().get("slots"));
            assertThrows(IllegalStateException.class, () -> dirty.retryFailed("core"));
            assertThrows(IllegalStateException.class, dirty::close);
            assertFalse(get(dirty.flushAndClose()).complete()); assertFalse(dirty.stats().closed());
        }
    }
    @Test void invalidMutationAndCapacityRejectionAreAtomic() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 2);
            dirty.markPatch("core", DocumentPatch.set("quest", Map.of("done", 1L))); dirty.markPatch("core", DocumentPatch.set("quest.done", 2L));
            var before = dirty.localView("core");
            assertThrows(RejectedExecutionException.class, () -> dirty.markPatch("core", DocumentPatch.set("quest", Map.of("done", 3L))));
            assertEquals(before, dirty.localView("core")); assertEquals(2, dirty.pendingOperations());
            Map<String,Object> bad = new HashMap<>(); bad.put("bad", new Object());
            assertThrows(IllegalArgumentException.class, () -> dirty.markReplace("core", bad));
            assertEquals(before, dirty.localView("core")); assertEquals(2, dirty.pendingOperations());
            dirty.markReplace("core", Map.of("gold", 8L));
            assertEquals(1, dirty.pendingOperations()); assertTrue(get(dirty.flushAndClose()).complete());
        }
    }
    @Test void staleLocalSnapshotCannotOverrideNewerChanges() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8); var view = dirty.localView("core");
            dirty.markPatch("core", inc("gold", 1));
            assertThrows(IllegalStateException.class, () -> dirty.markReplace("core", view.revision(), view.data().orElseThrow()));
            assertTrue(get(dirty.flushAndClose()).complete()); assertEquals(101L, dirty.current("core").orElseThrow().data().get("gold"));
        }
    }
    @Test void missingLoadInvalidParentsNullIncrementAndOverflowDoNotEnqueue() throws Exception {
        try (var store = new Store()) {
            var dirty = new DirtyDocumentSet(store, "player_component", "p", 2, 4);
            assertThrows(IllegalStateException.class, () -> dirty.markReplace("core", Map.of()));
            get(dirty.open("core"));
            assertThrows(IllegalStateException.class, () -> dirty.markPatch("core", inc("gold", 1)));
            Map<String,Object> data = new HashMap<>(); data.put("gold", null); data.put("max", Long.MAX_VALUE); data.put("scalar", 1L);
            dirty.markReplace("core", data);
            assertThrows(IllegalArgumentException.class, () -> dirty.markPatch("core", inc("gold", 1)));
            assertThrows(ArithmeticException.class, () -> dirty.markPatch("core", inc("max", 1)));
            assertThrows(IllegalArgumentException.class, () -> dirty.markPatch("core", DocumentPatch.set("scalar.child", 2)));
            assertEquals(1, dirty.pendingOperations()); assertTrue(get(dirty.flushAndClose()).complete());
        }
    }
    @Test void byteDocumentPathAndComponentBudgetsRejectWithoutChangingState() throws Exception {
        try (var store = new Store()) {
            var dirty = new DirtyDocumentSet(store, "player_component", "p",
                new DirtyDocumentSet.Limits(1, 8, 4096, 256, 1), () -> {});
            get(dirty.open("core")); dirty.markReplace("core", Map.of("x", 1L));
            assertThrows(RejectedExecutionException.class, () -> dirty.markReplace("core", Map.of("x", "a".repeat(512))));
            assertThrows(RejectedExecutionException.class, () -> dirty.markPatch("core", new DocumentPatch(Map.of("a",1L,"b",2L),Set.of(),Map.of())));
            assertThrows(RejectedExecutionException.class, () -> dirty.open("bag"));
            assertEquals(Map.of("x",1L), dirty.localView("core").data().orElseThrow());
            assertTrue(get(dirty.flushAndClose()).complete());
            var small = new DirtyDocumentSet(store, "other", "p", new DirtyDocumentSet.Limits(1,8,128,1024,8), () -> {});
            get(small.open("core"));
            assertThrows(RejectedExecutionException.class, () -> small.markReplace("core", Map.of("x",1L)));
            assertEquals(0, small.pendingOperations()); small.close();
        }
    }
    @Test void synchronousWriteThrowsCannotHangFlushAndOwnerCheckIsEnforced() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8); store.behavior = (k,c) -> { throw new IllegalStateException("driver"); };
            dirty.markPatch("core", inc("gold",1));
            assertEquals(DirtyDocumentSet.FailureKind.UNKNOWN, get(dirty.flush()).complete() ?
                null : dirty.failure("core").orElseThrow().kind());
            Thread owner = Thread.currentThread();
            var guarded = new DirtyDocumentSet(store, "other", "p", 1,4, () -> {
                if (Thread.currentThread() != owner) throw new IllegalStateException("wrong owner");
            });
            try (var executor = Executors.newSingleThreadExecutor()) {
                assertThrows(ExecutionException.class, () -> executor.submit(() -> guarded.open("core")).get(3,TimeUnit.SECONDS));
            }
            get(guarded.open("core")); guarded.close();
        }
    }
    @Test void repeatedIncrementsCoalesceAndNestedSetThenUnsetPreservesEmptyParent() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 8);
            for (int i = 0; i < 100; i++) dirty.markPatch("core", inc("gold", 1));
            assertEquals(1, dirty.pendingOperations());
            dirty.markPatch("core", DocumentPatch.set("empty.child", 1L));
            dirty.markPatch("core", new DocumentPatch(Map.of(), Set.of("empty.child"), Map.of()));
            assertEquals(2, dirty.pendingOperations());
            assertTrue(get(dirty.flushAndClose()).complete());
            assertEquals(200L, dirty.current("core").orElseThrow().data().get("gold"));
            assertEquals(Map.of(), dirty.current("core").orElseThrow().data().get("empty"));
            assertEquals(dirty.localView("core").data().orElseThrow(), get(store.load(dirty.key("core"))).orElseThrow().data());
        }
    }    @Test void synchronousLoadFailureCanRetryAndCloseCannotRacePendingLoad() throws Exception {
        var calls = new AtomicInteger(); var loading = new CompletableFuture<Optional<Snapshot>>();
        DocumentStore store = new DocumentStore() {
            public CompletionStage<Optional<Snapshot>> load(StoreKey key) {
                if (calls.incrementAndGet() == 1) throw new IllegalStateException("load");
                return loading;
            }
            public CompletionStage<WriteResult> write(StoreKey key, WriteCommand command) { throw new AssertionError(); }
            public CompletionStage<Void> flush(StoreKey key) { throw new AssertionError(); }
            public void close() {}
        };
        var dirty = new DirtyDocumentSet(store, "component", "p", 1, 4);
        assertThrows(ExecutionException.class, () -> get(dirty.open("core")));
        var second = dirty.open("core"); var coalesced = dirty.open("core");
        assertEquals(2, calls.get());
        assertThrows(IllegalStateException.class, dirty::close);
        assertThrows(IllegalStateException.class, dirty::flushAndClose);
        loading.complete(Optional.of(new Snapshot(1, Map.of("gold", 1L))));
        assertEquals(get(second), get(coalesced)); dirty.close();
    }
    @Test void seededSequenceMatchesUncoalescedReferenceAcrossRandomFlushBoundaries() throws Exception {
        try (var store = new Store()) {
            var dirty = created(store, 64);
            var reference = new StoreKey("reference", "p");
            long version = 1;
            get(store.real.write(reference, WriteCommand.create("reference-create", Map.of("gold",100L,"level",1L))));
            var random = new Random(726311);
            for (int i = 0; i < 300; i++) {
                if (i % 23 == 0) {
                    var image = Map.<String,Object>of("gold", (long)i, "p", Map.of("x",1L,"y",2L));
                    dirty.markReplace("core", image);
                    assertTrue(get(store.real.write(reference, WriteCommand.replace(version++, "r"+i, image))).successful());
                } else {
                    DocumentPatch patch = switch (random.nextInt(7)) {
                        case 0 -> DocumentPatch.set("p.x", (long)random.nextInt(100));
                        case 1 -> new DocumentPatch(Map.of(), Set.of("p.x"), Map.of());
                        case 2 -> inc("p.y", random.nextInt(9)-4);
                        case 3 -> DocumentPatch.set("p", Map.of("x",1L,"y",2L));
                        case 4 -> DocumentPatch.set("gold", (long)random.nextInt(100));
                        case 5 -> inc("gold", random.nextInt(9)-4);
                        default -> new DocumentPatch(Map.of(), Set.of("gold"), Map.of());
                    };
                    dirty.markPatch("core", patch);
                    assertTrue(get(store.real.write(reference, WriteCommand.patch(version++, "r"+i, patch))).successful());
                }
                if (random.nextInt(7) == 0) {
                    assertTrue(get(dirty.flush()).complete());
                    assertEquals(get(store.load(reference)).orElseThrow().data(), get(store.load(dirty.key("core"))).orElseThrow().data());
                }
            }
            assertTrue(get(dirty.flushAndClose()).complete());
            assertEquals(get(store.load(reference)).orElseThrow().data(), dirty.current("core").orElseThrow().data());
        }
    }
}
