package io.gameframe.storage;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Copies at submission time; no mutable entity or BSON object crosses threads. */
public final class Values {
    private Values() {}
    public static Map<String, Object> freeze(Map<String, ?> source) {
        Objects.requireNonNull(source);
        Map<String, Object> out = new TreeMap<>();
        source.forEach((key, value) -> {
            if (key == null || key.isEmpty() || key.contains(".") || key.startsWith("$") || key.indexOf(0) >= 0)
                throw new IllegalArgumentException("invalid field: " + key);
            out.put(key, freezeValue(value));
        });
        return Collections.unmodifiableMap(out);
    }
    public static Object freezeValue(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Integer || value instanceof Long) return value;
        if (value instanceof Double d && Double.isFinite(d)) return d;
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> typed = new HashMap<>();
            map.forEach((k, v) -> { if (!(k instanceof String)) throw new IllegalArgumentException("string keys required"); typed.put((String) k, v); });
            return freeze(typed);
        }
        if (value instanceof List<?> list) return Collections.unmodifiableList(list.stream().map(Values::freezeValue).toList());
        throw new IllegalArgumentException("unsupported persistent value: " + value.getClass());
    }
    public static String fingerprint(Object value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical(value).getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException e) { throw new AssertionError(e); }
    }
    private static String canonical(Object value) {
        if (value == null) return "null;";
        if (value instanceof Map<?, ?> map) {
            StringBuilder b = new StringBuilder("{");
            map.entrySet().stream().sorted(Comparator.comparing(e -> e.getKey().toString()))
                .forEach(e -> b.append(canonical(e.getKey())).append(canonical(e.getValue())));
            return b.append("}").toString();
        }
        if (value instanceof Collection<?> list) {
            StringBuilder b = new StringBuilder("[");
            list.forEach(v -> b.append(canonical(v)));
            return b.append("]").toString();
        }
        String s = value.toString(); return value.getClass().getSimpleName() + ":" + s.length() + ":" + s;
    }
}
