package io.gameframe.storage;

import java.nio.charset.StandardCharsets;
import java.util.*;

/** Map projection for coalescing, using the same contract as the document backends; arrays are whole values. */
public final class DocumentData {
    private DocumentData() {}
    public static Map<String, Object> apply(Map<String, Object> source, DocumentPatch patch) {
        Map<String, Object> data = mutable(source);
        patch.set().forEach((path, value) -> edit(data, path, value, false, false));
        patch.unset().forEach(path -> edit(data, path, null, true, false));
        patch.increment().forEach((path, value) -> edit(data, path, value, false, true));
        return Values.freeze(data);
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mutable(Map<String, Object> source) {
        Map<String, Object> out = new HashMap<>();
        source.forEach((k, v) -> out.put(k, v instanceof Map<?, ?> m ? mutable((Map<String, Object>) m) : v));
        return out;
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
            if (parent.containsKey(leaf) && !(prior instanceof Integer || prior instanceof Long))
                throw new IllegalArgumentException("increment requires integer: " + path);
            parent.put(leaf, Math.addExact(prior == null ? 0 : ((Number) prior).longValue(), (Long) value));
        } else parent.put(leaf, value);
    }
    /** Conservative logical payload budget, not a JVM heap or BSON byte count. */
    public static long estimatedBytes(Object value) { return size(value, 0); }
    private static long size(Object value, int depth) {
        if (depth > 64) throw new IllegalArgumentException("document nesting exceeds 64");
        if (value == null || value instanceof Boolean) return 8;
        if (value instanceof Number) return 16;
        if (value instanceof String s) return 16L + s.getBytes(StandardCharsets.UTF_8).length;
        long bytes = 32;
        if (value instanceof Map<?, ?> m) {
            for (var entry : m.entrySet()) bytes = Math.addExact(bytes,
                Math.addExact(size(entry.getKey(), depth + 1), size(entry.getValue(), depth + 1)));
        } else if (value instanceof Collection<?> c) {
            for (var entry : c) bytes = Math.addExact(bytes, size(entry, depth + 1));
        } else throw new IllegalArgumentException("unsupported document value");
        return bytes;
    }
}
