package io.gameframe.runtime;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/** Bounded structured runtime events with automatic redaction of sensitive fields. */
public final class RuntimeEventLogger {
    public enum Level { DEBUG, INFO, WARN, ERROR }

    public record Event(long timestampMillis, Level level, String name, Map<String, String> fields) {
        public Event {
            if (timestampMillis < 0) throw new IllegalArgumentException("timestampMillis must not be negative");
            Objects.requireNonNull(level, "level");
            requireName(name);
            fields = immutableFields(fields);
        }

        public String json() {
            var output = new StringBuilder("{\"timestampMillis\":").append(timestampMillis)
                    .append(",\"level\":\"").append(escape(level.name()))
                    .append("\",\"event\":\"").append(escape(name)).append("\",\"fields\":{");
            boolean first = true;
            for (var entry : fields.entrySet()) {
                if (!first) output.append(',');
                first = false;
                output.append('\"').append(escape(entry.getKey())).append("\":\"")
                        .append(escape(entry.getValue())).append('\"');
            }
            return output.append("}}\n").toString();
        }
    }

    private static final Consumer<Event> NOOP = ignored -> { };
    private final Consumer<Event> sink;
    private final int maxFields;
    private final int maxValueLength;

    public RuntimeEventLogger(Consumer<Event> sink) { this(sink, 32, 512); }

    public RuntimeEventLogger(Consumer<Event> sink, int maxFields, int maxValueLength) {
        this.sink = Objects.requireNonNull(sink, "sink");
        if (maxFields < 1 || maxValueLength < 1) throw new IllegalArgumentException("positive limits required");
        this.maxFields = maxFields;
        this.maxValueLength = maxValueLength;
    }

    public static RuntimeEventLogger noop() { return new RuntimeEventLogger(NOOP); }

    public void debug(String name, Map<String, ?> fields) { emit(Level.DEBUG, name, fields); }
    public void info(String name, Map<String, ?> fields) { emit(Level.INFO, name, fields); }
    public void warn(String name, Map<String, ?> fields) { emit(Level.WARN, name, fields); }
    public void error(String name, Map<String, ?> fields) { emit(Level.ERROR, name, fields); }

    public void emit(Level level, String name, Map<String, ?> fields) {
        Objects.requireNonNull(level, "level");
        Objects.requireNonNull(fields, "fields");
        if (fields.size() > maxFields) throw new IllegalArgumentException("too many structured log fields");
        var normalized = new LinkedHashMap<String, String>();
        fields.forEach((key, value) -> {
            requireFieldName(key);
            String text = String.valueOf(value == null ? "null" : value);
            if (isSensitive(key)) text = "[REDACTED]";
            if (text.length() > maxValueLength) throw new IllegalArgumentException("structured log value too long");
            normalized.put(key, text);
        });
        try {
            sink.accept(new Event(System.currentTimeMillis(), level, name, normalized));
        } catch (Throwable ignored) {
            // Logging must never break a server lifecycle or request path.
        }
    }

    private static Map<String, String> immutableFields(Map<String, String> fields) {
        Objects.requireNonNull(fields, "fields");
        var copy = new LinkedHashMap<String, String>();
        fields.forEach((key, value) -> {
            requireFieldName(key);
            if (value == null) throw new IllegalArgumentException("null structured log value");
            copy.put(key, value);
        });
        return Collections.unmodifiableMap(copy);
    }

    private static boolean isSensitive(String name) {
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("password") || lower.contains("secret") || lower.contains("token")
                || lower.contains("privatekey") || lower.endsWith("key");
    }

    private static void requireName(String name) {
        if (name == null || name.isBlank() || !name.matches("[a-zA-Z][a-zA-Z0-9_.-]*"))
            throw new IllegalArgumentException("invalid structured event name");
    }
    private static void requireFieldName(String name) {
        if (name == null || name.isBlank() || !name.matches("[a-zA-Z][a-zA-Z0-9_.-]*"))
            throw new IllegalArgumentException("invalid structured log field");
    }
    private static String escape(String value) {
        var output = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> output.append("\\\\");
                case '\"' -> output.append("\\\"");
                case '\n' -> output.append("\\n");
                case '\r' -> output.append("\\r");
                case '\t' -> output.append("\\t");
                default -> output.append(c < 0x20 ? '?' : c);
            }
        }
        return output.toString();
    }
}