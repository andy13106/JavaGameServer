package io.gameframe.storage;
import java.util.*;
public record WriteCommand(Kind kind, long expectedVersion, String operationId, Map<String, Object> data, DocumentPatch patch) {
    public enum Kind { CREATE, PATCH, REPLACE }
    public WriteCommand {
        Objects.requireNonNull(kind);
        if (operationId == null || operationId.isBlank() || operationId.length() > 128 || expectedVersion < 0 || expectedVersion == Long.MAX_VALUE)
            throw new IllegalArgumentException("invalid operation/version");
        if (kind == Kind.CREATE && expectedVersion != 0) throw new IllegalArgumentException("create version is zero");
        data = Values.freeze(data);
        if ((kind == Kind.PATCH) != (patch != null)) throw new IllegalArgumentException("patch required only for PATCH");
    }
    public static WriteCommand create(String op, Map<String, Object> data) { return new WriteCommand(Kind.CREATE, 0, op, data, null); }
    public static WriteCommand patch(long version, String op, DocumentPatch patch) { return new WriteCommand(Kind.PATCH, version, op, Map.of(), patch); }
    public static WriteCommand replace(long version, String op, Map<String, Object> data) { return new WriteCommand(Kind.REPLACE, version, op, data, null); }
    public String fingerprint() {
        return Values.fingerprint(List.of(kind.name(), expectedVersion, data,
            patch == null ? Map.of() : Map.of("set", patch.set(), "unset", patch.unset().stream().sorted().toList(), "inc", patch.increment())));
    }
}
