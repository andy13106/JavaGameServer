package io.gameframe.runtime;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Durable compare-and-set journal for cross-process actor migration.
 * Implementations must make create, compareAndSet and delete atomic for one
 * migration id. The version fences delayed messages and duplicate recovery
 * workers after a process restart.
 */
public interface MigrationJournal extends AutoCloseable {
    record Entry(String migrationId, String actorId, String sourceInstance,
                 String targetInstance, long fencingToken,
                 CrossProcessMigrationCoordinator.Phase phase, long version,
                 long updatedAtMillis, byte[] snapshot) {
        public Entry {
            requireText(migrationId, "migrationId");
            requireText(actorId, "actorId");
            requireText(sourceInstance, "sourceInstance");
            requireText(targetInstance, "targetInstance");
            Objects.requireNonNull(phase, "phase");
            if (sourceInstance.equals(targetInstance) || fencingToken < 1
                    || version < 1 || updatedAtMillis < 0) {
                throw new IllegalArgumentException("invalid migration journal entry");
            }
            snapshot = Objects.requireNonNull(snapshot, "snapshot").clone();
        }
        @Override public byte[] snapshot() { return snapshot.clone(); }
    }

    CompletionStage<Boolean> create(Entry entry);
    CompletionStage<Optional<Entry>> load(String migrationId);
    CompletionStage<Boolean> compareAndSet(String migrationId, long expectedVersion, Entry replacement);
    CompletionStage<List<Entry>> list(CrossProcessMigrationCoordinator.Phase phase, int maxEntries);
    CompletionStage<Boolean> delete(String migrationId, long expectedVersion);

    /** Stable, delimiter-safe representation shared by Redis and diagnostics. */
    static String encode(Entry entry) {
        Objects.requireNonNull(entry, "entry");
        return String.join("|", encodePart(entry.migrationId()), encodePart(entry.actorId()),
                encodePart(entry.sourceInstance()), encodePart(entry.targetInstance()),
                Long.toString(entry.fencingToken()), entry.phase().name(), Long.toString(entry.version()),
                Long.toString(entry.updatedAtMillis()),
                Base64.getUrlEncoder().withoutPadding().encodeToString(entry.snapshot()));
    }

    static Entry decode(String encoded) {
        if (encoded == null || encoded.isBlank()) throw new IllegalArgumentException("journal value required");
        String[] parts = encoded.split("\\|", -1);
        if (parts.length != 9) throw new IllegalArgumentException("invalid journal value");
        byte[] snapshot = parts[8].isEmpty() ? new byte[0] : Base64.getUrlDecoder().decode(parts[8]);
        return new Entry(decodePart(parts[0]), decodePart(parts[1]), decodePart(parts[2]), decodePart(parts[3]),
                Long.parseLong(parts[4]), CrossProcessMigrationCoordinator.Phase.valueOf(parts[5]),
                Long.parseLong(parts[6]), Long.parseLong(parts[7]), snapshot);
    }

    private static String encodePart(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
    private static String decodePart(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }
    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }
    @Override default void close() {}
}