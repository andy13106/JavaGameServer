package io.gameframe.storage;

import java.util.*;

/** Paths use dot notation. Arrays are replaced as a field; array-index updates are not supported. */
public record DocumentPatch(Map<String, Object> set, Set<String> unset, Map<String, Long> increment) {
    public DocumentPatch {
        TreeMap<String, Object> copy = new TreeMap<>();
        set.forEach((k, v) -> copy.put(k, Values.freezeValue(v)));
        set = Collections.unmodifiableMap(copy); unset = Set.copyOf(unset); increment = Map.copyOf(increment);
        List<String> paths = new ArrayList<>(set.keySet()); paths.addAll(unset); paths.addAll(increment.keySet());
        if (paths.isEmpty()) throw new IllegalArgumentException("empty patch");
        Collections.sort(paths);
        for (int i = 0; i < paths.size(); i++) {
            String p = paths.get(i);
            if (!p.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*"))
                throw new IllegalArgumentException("invalid patch path: " + p);
            if (i > 0 && (p.equals(paths.get(i - 1)) || p.startsWith(paths.get(i - 1) + ".")))
                throw new IllegalArgumentException("overlapping patch paths");
        }
    }
    public static DocumentPatch set(String path, Object value) {
        Map<String, Object> fields = new HashMap<>(); fields.put(path, value);
        return new DocumentPatch(fields, Set.of(), Map.of());
    }
}
