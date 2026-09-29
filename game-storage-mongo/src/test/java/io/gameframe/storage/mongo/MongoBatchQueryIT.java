package io.gameframe.storage.mongo;

import com.mongodb.MongoWriteException;
import com.mongodb.client.*;
import io.gameframe.storage.*;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;


@EnabledIfEnvironmentVariable(named = "GAME_TEST_MONGO_URI", matches = ".+")
class MongoBatchQueryIT {
    private static final String SECTION = "batch_component";
    private String database;
    private MongoClient observer;
    private MongoDocumentStore store;

    @BeforeEach void open() {
        String uri = System.getenv("GAME_TEST_MONGO_URI");
        database = "gameframe_batch_it_" + UUID.randomUUID().toString().replace("-", "");
        observer = MongoClients.create(uri);
        store = new MongoDocumentStore(uri, database, 2, 32, 8);
    }

    @AfterEach void close() {
        try {
            if (store != null) store.close();
        } finally {
            if (observer != null) {
                try { observer.getDatabase(database).drop(); }
                finally { observer.close(); }
            }
        }
    }

    static <T> T get(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(15, TimeUnit.SECONDS);
    }
    static StoreKey key(String id) { return new StoreKey(SECTION, id); }
    static MongoDocumentStore.BatchItem item(String id, WriteCommand command) {
        return new MongoDocumentStore.BatchItem(key(id), command);
    }
    static MongoDocumentStore.BatchItem create(String id, long level) {
        return item(id, WriteCommand.create("create-" + id, Map.of("level", level)));
    }
    void seed(int count) throws Exception {
        var items = new ArrayList<MongoDocumentStore.BatchItem>();
        for (int i = 0; i < count; i++) items.add(create("p" + i, i));
        assertTrue(get(store.writeBatch(items)).complete());
    }

    @Test void batchPreservesInputPositionsAndIsolatesCasConflictsAndMissingDocuments() throws Exception {
        seed(5);
        var batch = get(store.writeBatch(List.of(
            item("p0", WriteCommand.create("different-create", Map.of("level", 99L))),
            create("p5", 5),
            item("absent", WriteCommand.patch(1, "missing", DocumentPatch.set("level", 2L))),
            create("p6", 6))));
        assertFalse(batch.complete());
        assertEquals(List.of(0, 1, 2, 3), batch.items().stream().map(MongoDocumentStore.BatchItemResult::index).toList());
        assertEquals(List.of(key("p0"), key("p5"), key("absent"), key("p6")),
            batch.items().stream().map(MongoDocumentStore.BatchItemResult::key).toList());
        assertEquals(WriteResult.Status.CONFLICT, batch.items().get(0).result().status());
        assertEquals(WriteResult.Status.MISSING, batch.items().get(2).result().status());
        assertTrue(batch.items().get(1).successful());
        assertTrue(batch.items().get(3).successful());
        assertEquals(2, batch.failures().size());
        assertEquals(0L, get(store.load(key("p0"))).orElseThrow().data().get("level"));
        assertEquals(6L, get(store.load(key("p6"))).orElseThrow().data().get("level"));
    }

    @Test void pagesHandleEmptyPartialExactAndMultiplePagesWithoutPhantomNextPage() throws Exception {
        assertTrue(get(store.page(SECTION, null, 2)).items().isEmpty());
        assertFalse(get(store.page(SECTION, null, 2)).hasNext());
        seed(4);
        var first = get(store.page(SECTION, null, 2));
        assertEquals(List.of("p0", "p1"), first.items().stream().map(MongoDocumentStore.DocumentRecord::id).toList());
        assertEquals("p1", first.nextCursor());
        var last = get(store.page(SECTION, first.nextCursor(), 2));
        assertEquals(List.of("p2", "p3"), last.items().stream().map(MongoDocumentStore.DocumentRecord::id).toList());
        assertFalse(last.hasNext());
        assertFalse(get(store.page(SECTION, null, 4)).hasNext());
        assertFalse(get(store.page(SECTION, "p3", 2)).hasNext());
        var partial = get(store.page(SECTION, "p2", 2));
        assertEquals(1, partial.items().size());
        assertEquals(1L, partial.items().getFirst().snapshot().version());
        assertEquals(3L, partial.items().getFirst().snapshot().data().get("level"));
        assertFalse(partial.hasNext());
        assertThrows(UnsupportedOperationException.class, () -> partial.items().clear());

        var ids = new ArrayList<String>();
        String cursor = null;
        do {
            var page = get(store.page(SECTION, cursor, 1));
            ids.addAll(page.items().stream().map(MongoDocumentStore.DocumentRecord::id).toList());
            cursor = page.nextCursor();
        } while (cursor != null);
        assertEquals(List.of("p0", "p1", "p2", "p3"), ids);
    }

    @Test void indexesKeepFullFieldIdentityAndDirectionDespiteCommonOrLongPrefixes() throws Exception {
        String longPrefix = "data." + "a".repeat(140);
        var specs = List.of(
            new MongoDocumentStore.IndexSpec(SECTION, "data.a_b", true, false),
            new MongoDocumentStore.IndexSpec(SECTION, "data.a.b", true, false),
            new MongoDocumentStore.IndexSpec(SECTION, longPrefix + "x", true, false),
            new MongoDocumentStore.IndexSpec(SECTION, longPrefix + "y", true, false),
            new MongoDocumentStore.IndexSpec(SECTION, longPrefix + "x", false, false),
            new MongoDocumentStore.IndexSpec(SECTION, "data.account", true, true));
        var names = new HashSet<String>();
        for (var spec : specs) {
            String name = get(store.ensureIndex(spec));
            assertEquals(name, get(store.ensureIndex(spec)));
            assertTrue(name.length() <= 120);
            assertTrue(names.add(name), "index name collision");
            var actual = observer.getDatabase(database).getCollection(SECTION).listIndexes()
                .into(new ArrayList<>()).stream().filter(d -> name.equals(d.getString("name"))).findFirst().orElseThrow();
            assertEquals(new Document(spec.field(), spec.ascending() ? 1 : -1), actual.get("key"));
            assertEquals(spec.unique(), Boolean.TRUE.equals(actual.getBoolean("unique")));
        }
    }

    @Test void uniqueIndexViolationsRemainErrorsAndDoNotHideOtherItemSuccess() throws Exception {
        get(store.ensureIndex(new MongoDocumentStore.IndexSpec(SECTION, "data.level", true, true)));
        assertTrue(get(store.writeBatch(List.of(create("p0", 10), create("p1", 20)))).complete());
        var result = get(store.writeBatch(List.of(
            create("duplicate", 10),
            item("p1", WriteCommand.patch(1, "bad-patch", DocumentPatch.set("level", 10L))),
            item("p1", WriteCommand.replace(1, "bad-replace", Map.of("level", 10L))),
            create("p2", 30))));
        assertEquals(3, result.failures().size());
        for (var failed : result.failures()) {
            assertNull(failed.result());
            var error = assertInstanceOf(StorageException.class, failed.error());
            assertEquals(StorageException.Outcome.UNKNOWN, error.outcome());
            var cause = assertInstanceOf(MongoWriteException.class, error.getCause());
            assertEquals(11000, cause.getError().getCode());
        }
        assertTrue(result.items().getLast().successful());
        assertTrue(get(store.load(key("duplicate"))).isEmpty());
        assertEquals(20L, get(store.load(key("p1"))).orElseThrow().data().get("level"));
        assertEquals(1L, get(store.load(key("p1"))).orElseThrow().version());
    }

    @Test void sameKeyWritesRemainOrderedAndReplayRequiresOriginalVersionOperationAndFingerprint() throws Exception {
        var increment = new DocumentPatch(Map.of(), Set.of(), Map.of("level", 2L));
        var commands = List.of(create("p0", 1),
            item("p0", WriteCommand.patch(1, "patch", increment)),
            item("p0", WriteCommand.replace(2, "replace", Map.of("level", 10L))),
            item("p0", WriteCommand.patch(3, "last-patch", increment)));
        var first = get(store.writeBatch(commands));
        assertTrue(first.complete());
        assertEquals(List.of(1L, 2L, 3L, 4L),
            first.items().stream().map(i -> i.result().version()).toList());
        var replay = get(store.writeBatch(List.of(commands.getLast())));
        assertEquals(WriteResult.Status.ALREADY_APPLIED, replay.items().getFirst().result().status());
        var mismatch = get(store.writeBatch(List.of(
            item("p0", WriteCommand.patch(3, "last-patch", DocumentPatch.set("level", 999L))),
            commands.get(1),
            item("p0", WriteCommand.patch(4, "after-conflicts", increment)))));
        assertEquals(WriteResult.Status.CONFLICT, mismatch.items().get(0).result().status());
        assertEquals(WriteResult.Status.CONFLICT, mismatch.items().get(1).result().status());
        assertTrue(mismatch.items().get(2).successful());
        assertEquals(14L, get(store.load(key("p0"))).orElseThrow().data().get("level"));
    }

    @Test void invalidInputIsRejectedBeforeAnyWritesAndPayloadsAreFrozen() throws Exception {
        var bad = new ArrayList<MongoDocumentStore.BatchItem>();
        bad.add(create("must-not-exist", 1)); bad.add(null);
        assertThrows(NullPointerException.class, () -> store.writeBatch(bad));
        assertTrue(get(store.load(key("must-not-exist"))).isEmpty());
        assertThrows(IllegalArgumentException.class,
            () -> store.writeBatch(Collections.nCopies(9, create("too-many", 1))));
        assertTrue(get(store.load(key("too-many"))).isEmpty());
        for (String field : List.of("data.", "data..a", "data.$bad", "data.1bad", ".data", "a".repeat(513))) {
            assertThrows(IllegalArgumentException.class,
                () -> new MongoDocumentStore.IndexSpec(SECTION, field, true, false));
        }
        assertThrows(IllegalArgumentException.class, () -> store.page("bad-section!", null, 2));
        assertThrows(IllegalArgumentException.class, () -> store.page(SECTION, "", 2));
        assertThrows(IllegalArgumentException.class, () -> store.page(SECTION, null, 0));
        assertThrows(IllegalArgumentException.class, () -> store.page(SECTION, null, 9));
        assertTrue(get(store.writeBatch(List.of())).complete());

        var payload = new HashMap<String, Object>();
        payload.put("level", 7L);
        var frozen = item("frozen", WriteCommand.create("frozen-create", payload));
        payload.put("level", 999L);
        var input = new ArrayList<>(List.of(frozen));
        var pending = store.writeBatch(input);
        input.clear();
        var result = get(pending);
        assertTrue(result.complete());
        assertThrows(UnsupportedOperationException.class, () -> result.items().clear());
        assertEquals(7L, get(store.load(key("frozen"))).orElseThrow().data().get("level"));
    }

    @Test void closedQueueReportsNotExecutedForEveryItemAndCreatesNoDocuments() throws Exception {
        store.close();
        var result = get(store.writeBatch(List.of(create("closed0", 1), create("closed1", 2))));
        assertEquals(2, result.failures().size());
        for (var failed : result.failures()) {
            assertNull(failed.result());
            assertEquals(StorageException.Outcome.NOT_EXECUTED,
                assertInstanceOf(StorageException.class, failed.error()).outcome());
        }
        assertEquals(0L, observer.getDatabase(database).getCollection(SECTION).countDocuments());
    }
}