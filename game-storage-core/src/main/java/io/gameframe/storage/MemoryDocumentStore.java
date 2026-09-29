package io.gameframe.storage;

import java.util.*;
import java.util.concurrent.*;

/** Deterministic development backend. Volatile: restart loses all data. */
public final class MemoryDocumentStore implements DocumentStore {
    private record Stored(Snapshot snapshot, String operation, String fingerprint) {}
    private final ConcurrentMap<StoreKey, Stored> documents = new ConcurrentHashMap<>();
    private final OrderedExecutor executor;
    public MemoryDocumentStore(int lanes, int capacity) { executor = new OrderedExecutor("memory-store", lanes, capacity); }
    public CompletionStage<Optional<Snapshot>> load(StoreKey key) {
        return executor.submit(key, () -> Optional.ofNullable(documents.get(key)).map(Stored::snapshot));
    }
    public CompletionStage<WriteResult> write(StoreKey key, WriteCommand command) {
        Objects.requireNonNull(command);
        return executor.submit(key, () -> {
            Stored old = documents.get(key);
            if (old != null && old.snapshot.version() == command.expectedVersion() + 1 &&
                    old.operation.equals(command.operationId()) && old.fingerprint.equals(command.fingerprint()))
                return new WriteResult(WriteResult.Status.ALREADY_APPLIED, old.snapshot.version());
            if (command.kind() == WriteCommand.Kind.CREATE ? old != null : old == null || old.snapshot.version() != command.expectedVersion())
                return new WriteResult(old == null ? WriteResult.Status.MISSING : WriteResult.Status.CONFLICT, old == null ? 0 : old.snapshot.version());
            Map<String, Object> data;
            if (command.kind() == WriteCommand.Kind.PATCH) {
                data = mutable(old.snapshot.data());
                command.patch().set().forEach((path, value) -> edit(data, path, value, false, false));
                command.patch().unset().forEach(path -> edit(data, path, null, true, false));
                command.patch().increment().forEach((path, value) -> edit(data, path, value, false, true));
            } else data = command.data();
            long version = command.expectedVersion() + 1;
            documents.put(key, new Stored(new Snapshot(version, data), command.operationId(), command.fingerprint()));
            return new WriteResult(WriteResult.Status.APPLIED, version);
        });
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mutable(Map<String, Object> map) {
        Map<String, Object> copy = new HashMap<>();
        map.forEach((k, v) -> copy.put(k, v instanceof Map<?, ?> m ? mutable((Map<String,Object>) m) : v));
        return copy;
    }
    @SuppressWarnings("unchecked")
    private static void edit(Map<String, Object> data, String path, Object value, boolean remove, boolean increment) {
        String[] parts = path.split("\\.");
        Map<String, Object> parent = data;
        for (int i = 0; i < parts.length - 1; i++) {
            Object next = parent.get(parts[i]);
            if (next == null && !parent.containsKey(parts[i])) {
                if (remove) return;
                next = new HashMap<String, Object>(); parent.put(parts[i], next);
            }
            if (!(next instanceof Map)) throw new IllegalArgumentException("non-object parent: " + path);
            parent = (Map<String, Object>) next;
        }
        String leaf = parts[parts.length - 1];
        if (remove) parent.remove(leaf);
        else if (increment) {
            Object prior = parent.get(leaf);
            if (parent.containsKey(leaf) && !(prior instanceof Integer || prior instanceof Long)) throw new IllegalArgumentException("increment requires integer");
            parent.put(leaf, Math.addExact(prior == null ? 0 : ((Number) prior).longValue(), (Long) value));
        } else parent.put(leaf, value);
    }
    public CompletionStage<Void> flush(StoreKey key) { return executor.submit(key, () -> null); }
    public void close() { executor.close(); }
}
