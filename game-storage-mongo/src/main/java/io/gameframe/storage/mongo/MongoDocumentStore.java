package io.gameframe.storage.mongo;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import io.gameframe.storage.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.*;
import java.util.concurrent.*;
import static com.mongodb.client.model.Filters.*;

/** Mongo CAS document store plus explicitly non-atomic batch, page and index operations. */
public final class MongoDocumentStore implements DocumentStore {
    public record BatchItem(StoreKey key, WriteCommand command) {
        public BatchItem { Objects.requireNonNull(key); Objects.requireNonNull(command); }
    }
    public record BatchItemResult(int index, StoreKey key, WriteResult result, Throwable error) {
        public boolean successful() { return error == null && result != null && result.successful(); }
    }
    public record BatchResult(List<BatchItemResult> items) {
        public BatchResult { items = List.copyOf(items); }
        public boolean complete() { return items.stream().allMatch(BatchItemResult::successful); }
        public List<BatchItemResult> failures() { return items.stream().filter(i -> !i.successful()).toList(); }
    }
    public record DocumentRecord(String id, Snapshot snapshot) {}
    public record Page(List<DocumentRecord> items, String nextCursor) {
        public Page { items = List.copyOf(items); }
        public boolean hasNext() { return nextCursor != null; }
    }
    public record IndexSpec(String section, String field, boolean ascending, boolean unique) {
        public IndexSpec {
            if (section == null || !section.matches("[a-z][a-z0-9_]{0,63}"))
                throw new IllegalArgumentException("invalid section");
            if (field == null || field.length() > 512 ||
                !field.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*"))
                throw new IllegalArgumentException("invalid index field");
        }
    }

    private final MongoClient client;
    private final MongoDatabase database;
    private final OrderedExecutor executor;
    private final int maxBatch;
    public MongoDocumentStore(String uri, String databaseName, int lanes, int capacity) {
        this(uri, databaseName, lanes, capacity, 256);
    }
    public MongoDocumentStore(String uri, String databaseName, int lanes, int capacity, int maxBatch) {
        if (lanes < 1 || capacity < 1 || maxBatch < 1 || maxBatch == Integer.MAX_VALUE || databaseName == null || databaseName.isBlank())
            throw new IllegalArgumentException();
        var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
            .applyToConnectionPoolSettings(b -> b.maxSize(lanes).minSize(0).maxWaitTime(5, TimeUnit.SECONDS))
            .applyToClusterSettings(b -> b.serverSelectionTimeout(5, TimeUnit.SECONDS))
            .applyToSocketSettings(b -> b.connectTimeout(5, TimeUnit.SECONDS).readTimeout(10, TimeUnit.SECONDS))
            .writeConcern(WriteConcern.MAJORITY.withWTimeout(5, TimeUnit.SECONDS))
            .readPreference(ReadPreference.primary()).build();
        client = MongoClients.create(settings);
        database = client.getDatabase(databaseName);
        executor = new OrderedExecutor("mongo-store", lanes, capacity);
        this.maxBatch = maxBatch;
    }

    private MongoCollection<Document> collection(StoreKey key) { return database.getCollection(key.section()); }
    private MongoCollection<Document> collection(String section) {
        if (section == null || !section.matches("[a-z][a-z0-9_]{0,63}")) throw new IllegalArgumentException("invalid section");
        return database.getCollection(section);
    }
    public CompletionStage<Optional<Snapshot>> load(StoreKey key) {
        return executor.submit(key, () -> {
            Document doc = collection(key).find(eq("_id", key.id())).first();
            return Optional.ofNullable(doc).map(MongoDocumentStore::snapshot);
        });
    }
    public CompletionStage<WriteResult> write(StoreKey key, WriteCommand command) {
        Objects.requireNonNull(command);
        return executor.submit(key, () -> perform(key, command));
    }

    /**
     * Executes each item independently. The batch is not a transaction: a queue rejection,
     * CAS conflict or driver uncertainty is reported only for that item.
     * This aggregates single-document writes; it does not use Mongo bulkWrite or reduce round trips.
     * Later items continue after an earlier failure, including items with the same key.
     */
    public CompletionStage<BatchResult> writeBatch(List<BatchItem> input) {
        Objects.requireNonNull(input);
        if (input.isEmpty()) return CompletableFuture.completedFuture(new BatchResult(List.of()));
        if (input.size() > maxBatch) throw new IllegalArgumentException("batch exceeds maxBatch");
        List<BatchItem> items = List.copyOf(input);
        List<CompletableFuture<BatchItemResult>> futures = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            int index = i; BatchItem item = items.get(i);
            CompletionStage<WriteResult> operation;
            try { operation = write(item.key(), item.command()); }
            catch (Throwable error) { operation = CompletableFuture.failedFuture(error); }
            futures.add(operation.handle((result, error) ->
                new BatchItemResult(index, item.key(), result, error == null ? null : unwrap(error))).toCompletableFuture());
        }
        var result = new CompletableFuture<BatchResult>();
        CompletableFuture.allOf(futures.toArray(CompletableFuture<?>[]::new)).whenComplete((ignored, error) ->
            result.complete(new BatchResult(futures.stream().map(CompletableFuture::join).toList())));
        return result.minimalCompletionStage();
    }

    /** Ascending string _id page, with an exclusive last-ID cursor and no cross-page snapshot. */
    public CompletionStage<Page> page(String section, String afterId, int limit) {
        var collection = collection(section);
        if (limit < 1 || limit > maxBatch) throw new IllegalArgumentException("invalid page limit");
        if (afterId != null && afterId.isBlank()) throw new IllegalArgumentException("blank cursor");
        return executor.submit("page:" + section, () -> {
            var query = afterId == null ? new Document() : gt("_id", afterId);
            var docs = collection.find(query).sort(Sorts.ascending("_id"))
                .collation(Collation.builder().locale("simple").build())
                .maxTime(5, TimeUnit.SECONDS).limit(limit + 1).into(new ArrayList<>());
            List<DocumentRecord> records = docs.stream().limit(limit)
                .map(d -> new DocumentRecord(d.getString("_id"), snapshot(d))).toList();
            String next = docs.size() > limit ? records.getLast().id() : null;
            return new Page(records, next);
        });
    }

    /** Explicit index configuration; repeated calls with the same spec are idempotent. */
    public CompletionStage<String> ensureIndex(IndexSpec spec) {
        Objects.requireNonNull(spec);
        return executor.submit("index:" + spec.section(), () -> {
            String name = indexName(spec);
            var keys = spec.ascending() ? Indexes.ascending(spec.field()) : Indexes.descending(spec.field());
            return collection(spec.section()).createIndex(keys, new IndexOptions().name(name).unique(spec.unique()));
        });
    }
    public int maxBatch() { return maxBatch; }

    private WriteResult perform(StoreKey key, WriteCommand command) {
        var collection = collection(key);
        long nextVersion = command.expectedVersion() + 1;
        String fingerprint = command.fingerprint();
        try {
            if (command.kind() == WriteCommand.Kind.CREATE) {
                try { collection.insertOne(envelope(key, command, nextVersion, fingerprint)); }
                catch (MongoWriteException e) {
                    if (e.getError().getCode() == 11000) {
                        var resolved = resolve(key, command, fingerprint);
                        // A secondary unique index can reject a new ID. Preserve the server error.
                        if (resolved.status() != WriteResult.Status.MISSING) return resolved;
                    }
                    throw e;
                }
                return new WriteResult(WriteResult.Status.APPLIED, nextVersion);
            }
            Bson filter = and(eq("_id", key.id()), eq("_version", command.expectedVersion()));
            long matched;
            if (command.kind() == WriteCommand.Kind.REPLACE) {
                matched = collection.replaceOne(filter, envelope(key, command, nextVersion, fingerprint)).getMatchedCount();
            } else {
                List<Bson> updates = new ArrayList<>();
                command.patch().set().forEach((path, value) -> updates.add(Updates.set("data." + path, bson(value))));
                command.patch().unset().forEach(path -> updates.add(Updates.unset("data." + path)));
                command.patch().increment().forEach((path, value) -> updates.add(Updates.inc("data." + path, value)));
                updates.add(Updates.set("_version", nextVersion));
                updates.add(Updates.set("_operation", command.operationId()));
                updates.add(Updates.set("_fingerprint", fingerprint));
                matched = collection.updateOne(filter, Updates.combine(updates)).getMatchedCount();
            }
            return matched == 1 ? new WriteResult(WriteResult.Status.APPLIED, nextVersion) : resolve(key, command, fingerprint);
        } catch (MongoException e) {
            throw new StorageException(StorageException.Outcome.UNKNOWN, "Mongo write outcome requires reconciliation", e);
        }
    }
    private WriteResult resolve(StoreKey key, WriteCommand command, String fingerprint) {
        Document current = collection(key).find(eq("_id", key.id()))
            .projection(Projections.include("_version", "_operation", "_fingerprint")).first();
        if (current == null) return new WriteResult(WriteResult.Status.MISSING, 0);
        long version = ((Number) current.get("_version")).longValue();
        boolean same = version == command.expectedVersion() + 1 && command.operationId().equals(current.getString("_operation"))
            && fingerprint.equals(current.getString("_fingerprint"));
        return new WriteResult(same ? WriteResult.Status.ALREADY_APPLIED : WriteResult.Status.CONFLICT, version);
    }
    private static Snapshot snapshot(Document document) {
        return new Snapshot(((Number) document.get("_version")).longValue(), document.get("data", Document.class));
    }
    private static Document envelope(StoreKey key, WriteCommand command, long version, String fingerprint) {
        return new Document("_id", key.id()).append("_version", version).append("_operation", command.operationId())
            .append("_fingerprint", fingerprint).append("data", bson(command.data()));
    }
    private static String indexName(IndexSpec spec) {
        String label = spec.field().replace('.', '_');
        if (label.length() > 32) label = label.substring(0, 32);
        String hash = Values.fingerprint(List.of(spec.section(), spec.field(), spec.ascending(), spec.unique()));
        return "gf_" + label + (spec.ascending() ? "_asc_" : "_desc_") +
            (spec.unique() ? "uniq_" : "") + hash;
    }
    private static Object bson(Object value) {
        if (value instanceof Map<?, ?> map) {
            Document result = new Document(); map.forEach((k, v) -> result.put((String) k, bson(v))); return result;
        }
        if (value instanceof List<?> list) return list.stream().map(MongoDocumentStore::bson).toList();
        return value;
    }
    public CompletionStage<Void> flush(StoreKey key) { return executor.submit(key, () -> null); }
    public void close() { executor.close(); client.close(); }
    private static Throwable unwrap(Throwable error) {
        return (error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null ?
            error.getCause() : error;
    }
}
