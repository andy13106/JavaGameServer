package io.gameframe.storage.redis;

import io.gameframe.runtime.CrossProcessMigrationCoordinator;
import io.gameframe.runtime.MigrationJournal;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Redis-backed migration journal with TTL retention and version fencing. */
public final class RedisMigrationJournal implements MigrationJournal {
    private final RedisStore redis;
    private final String namespace;
    private final Duration retention;
    private final int maxSnapshotBytes;

    public RedisMigrationJournal(RedisStore redis) {
        this(redis, "gameframe:migration", Duration.ofDays(7), 2 * 1024 * 1024);
    }
    public RedisMigrationJournal(RedisStore redis, String namespace, Duration retention, int maxSnapshotBytes) {
        this.redis = Objects.requireNonNull(redis, "redis");
        if (namespace == null || namespace.isBlank() || retention == null || retention.toMillis() < 1
                || maxSnapshotBytes < 1) throw new IllegalArgumentException("invalid journal configuration");
        this.namespace = namespace;
        this.retention = retention;
        this.maxSnapshotBytes = maxSnapshotBytes;
    }

    @Override public CompletionStage<Boolean> create(Entry entry) {
        validate(entry);
        return redis.setIfAbsent(key(entry.migrationId()), MigrationJournal.encode(entry), retention);
    }

    @Override public CompletionStage<Optional<Entry>> load(String migrationId) {
        String key = key(migrationId);
        return redis.get(key).thenApply(value -> decodeOptional(value));
    }

    @Override public CompletionStage<Boolean> compareAndSet(String migrationId, long expectedVersion, Entry replacement) {
        if (expectedVersion < 1) throw new IllegalArgumentException("expectedVersion must be positive");
        validate(replacement);
        if (!replacement.migrationId().equals(migrationId) || replacement.version() != expectedVersion + 1) {
            throw new IllegalArgumentException("replacement must advance the expected version");
        }
        return load(migrationId).thenCompose(current -> {
            if (current.isEmpty() || current.get().version() != expectedVersion) return CompletableFuture.completedFuture(false);
            return redis.compareAndSet(key(migrationId), MigrationJournal.encode(current.get()),
                    MigrationJournal.encode(replacement), retention);
        });
    }

    @Override public CompletionStage<List<Entry>> list(CrossProcessMigrationCoordinator.Phase phase, int maxEntries) {
        Objects.requireNonNull(phase, "phase");
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
        return redis.scanKeys(namespace + ":*", maxEntries).thenCompose(keys -> {
            var ordered = keys.stream().sorted().toList();
            var reads = ordered.stream().map(key -> redis.get(key).toCompletableFuture()).toList();
            return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).thenApply(ignored ->
                    reads.stream().map(CompletableFuture::join).map(this::decodeOptional).flatMap(Optional::stream)
                            .filter(entry -> entry.phase() == phase)
                            .sorted(Comparator.comparingLong(Entry::updatedAtMillis))
                            .limit(maxEntries).toList());
        });
    }

    @Override public CompletionStage<Boolean> delete(String migrationId, long expectedVersion) {
        if (expectedVersion < 1) throw new IllegalArgumentException("expectedVersion must be positive");
        return load(migrationId).thenCompose(current -> {
            if (current.isEmpty() || current.get().version() != expectedVersion) return CompletableFuture.completedFuture(false);
            return redis.compareAndDelete(key(migrationId), MigrationJournal.encode(current.get()));
        });
    }

    public String key(String migrationId) {
        if (migrationId == null || migrationId.isBlank()) throw new IllegalArgumentException("migrationId required");
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(migrationId.getBytes(StandardCharsets.UTF_8));
        return namespace + ":" + encoded;
    }

    private Optional<Entry> decodeOptional(String value) {
        if (value == null) return Optional.empty();
        try {
            Entry entry = MigrationJournal.decode(value);
            validate(entry);
            return Optional.of(entry);
        } catch (RuntimeException malformed) {
            return Optional.empty();
        }
    }
    private void validate(Entry entry) {
        Objects.requireNonNull(entry, "entry");
        if (entry.snapshot().length > maxSnapshotBytes) throw new IllegalArgumentException("snapshot exceeds journal limit");
    }
}