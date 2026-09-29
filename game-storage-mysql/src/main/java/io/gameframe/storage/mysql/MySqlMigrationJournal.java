package io.gameframe.storage.mysql;

import io.gameframe.runtime.CrossProcessMigrationCoordinator;
import io.gameframe.runtime.MigrationJournal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Explicit-SQL durable migration journal with optimistic version fencing. */
public final class MySqlMigrationJournal implements MigrationJournal {
    private final MySqlStore mysql;
    private final int maxSnapshotBytes;

    public MySqlMigrationJournal(MySqlStore mysql) { this(mysql, 2 * 1024 * 1024); }
    public MySqlMigrationJournal(MySqlStore mysql, int maxSnapshotBytes) {
        this.mysql = Objects.requireNonNull(mysql, "mysql");
        if (maxSnapshotBytes < 1) throw new IllegalArgumentException("maxSnapshotBytes must be positive");
        this.maxSnapshotBytes = maxSnapshotBytes;
    }

    public CompletionStage<Void> initializeSchema() {
        return mysql.execute("migration-journal-schema", connection -> {
            try (var statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS game_migration_journal (
                          migration_id VARCHAR(128) NOT NULL,
                          actor_id VARCHAR(128) NOT NULL,
                          source_instance VARCHAR(128) NOT NULL,
                          target_instance VARCHAR(128) NOT NULL,
                          fencing_token BIGINT NOT NULL,
                          phase VARCHAR(32) NOT NULL,
                          version BIGINT NOT NULL,
                          updated_at_millis BIGINT NOT NULL,
                          snapshot MEDIUMBLOB NOT NULL,
                          PRIMARY KEY (migration_id),
                          KEY idx_game_migration_phase_updated (phase, updated_at_millis)
                        ) ENGINE=InnoDB
                        """);
            }
            return null;
        });
    }

    @Override public CompletionStage<Boolean> create(Entry entry) {
        validate(entry);
        return mysql.execute("migration-journal-create:" + entry.migrationId(), connection -> {
            try (var statement = connection.prepareStatement("""
                    INSERT INTO game_migration_journal
                    (migration_id, actor_id, source_instance, target_instance, fencing_token,
                     phase, version, updated_at_millis, snapshot)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """)) {
                bind(statement, entry);
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override public CompletionStage<Optional<Entry>> load(String migrationId) {
        requireText(migrationId, "migrationId");
        return mysql.execute("migration-journal-load:" + migrationId, connection -> {
            try (var statement = connection.prepareStatement("""
                    SELECT migration_id, actor_id, source_instance, target_instance, fencing_token,
                           phase, version, updated_at_millis, snapshot
                    FROM game_migration_journal WHERE migration_id=?
                    """)) {
                statement.setString(1, migrationId);
                try (ResultSet rows = statement.executeQuery()) {
                    return rows.next() ? Optional.of(read(rows)) : Optional.empty();
                }
            }
        });
    }

    @Override public CompletionStage<Boolean> compareAndSet(String migrationId, long expectedVersion, Entry replacement) {
        requireText(migrationId, "migrationId");
        if (expectedVersion < 1) throw new IllegalArgumentException("expectedVersion must be positive");
        validate(replacement);
        if (!replacement.migrationId().equals(migrationId) || replacement.version() != expectedVersion + 1) {
            throw new IllegalArgumentException("replacement must advance the expected version");
        }
        return mysql.execute("migration-journal-cas:" + migrationId, connection -> {
            try (var statement = connection.prepareStatement("""
                    UPDATE game_migration_journal
                    SET actor_id=?, source_instance=?, target_instance=?, fencing_token=?, phase=?,
                        version=?, updated_at_millis=?, snapshot=?
                    WHERE migration_id=? AND version=?
                    """)) {
                statement.setString(1, replacement.actorId());
                statement.setString(2, replacement.sourceInstance());
                statement.setString(3, replacement.targetInstance());
                statement.setLong(4, replacement.fencingToken());
                statement.setString(5, replacement.phase().name());
                statement.setLong(6, replacement.version());
                statement.setLong(7, replacement.updatedAtMillis());
                statement.setBytes(8, replacement.snapshot());
                statement.setString(9, migrationId);
                statement.setLong(10, expectedVersion);
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override public CompletionStage<List<Entry>> list(CrossProcessMigrationCoordinator.Phase phase, int maxEntries) {
        Objects.requireNonNull(phase, "phase");
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
        return mysql.execute("migration-journal-list:" + phase.name(), connection -> {
            try (var statement = connection.prepareStatement("""
                    SELECT migration_id, actor_id, source_instance, target_instance, fencing_token,
                           phase, version, updated_at_millis, snapshot
                    FROM game_migration_journal WHERE phase=?
                    ORDER BY updated_at_millis, migration_id LIMIT ?
                    """)) {
                statement.setString(1, phase.name());
                statement.setInt(2, maxEntries);
                var result = new ArrayList<Entry>();
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) result.add(read(rows));
                }
                return List.copyOf(result);
            }
        });
    }

    @Override public CompletionStage<Boolean> delete(String migrationId, long expectedVersion) {
        requireText(migrationId, "migrationId");
        if (expectedVersion < 1) throw new IllegalArgumentException("expectedVersion must be positive");
        return mysql.execute("migration-journal-delete:" + migrationId, connection -> {
            try (var statement = connection.prepareStatement("""
                    DELETE FROM game_migration_journal WHERE migration_id=? AND version=?
                    """)) {
                statement.setString(1, migrationId);
                statement.setLong(2, expectedVersion);
                return statement.executeUpdate() == 1;
            }
        });
    }

    private void validate(Entry entry) {
        Objects.requireNonNull(entry, "entry");
        if (entry.snapshot().length > maxSnapshotBytes) throw new IllegalArgumentException("snapshot exceeds journal limit");
    }
    private static void bind(java.sql.PreparedStatement statement, Entry entry) throws SQLException {
        statement.setString(1, entry.migrationId()); statement.setString(2, entry.actorId());
        statement.setString(3, entry.sourceInstance()); statement.setString(4, entry.targetInstance());
        statement.setLong(5, entry.fencingToken()); statement.setString(6, entry.phase().name());
        statement.setLong(7, entry.version()); statement.setLong(8, entry.updatedAtMillis());
        statement.setBytes(9, entry.snapshot());
    }
    private static Entry read(ResultSet rows) throws SQLException {
        return new Entry(rows.getString(1), rows.getString(2), rows.getString(3), rows.getString(4),
                rows.getLong(5), CrossProcessMigrationCoordinator.Phase.valueOf(rows.getString(6)),
                rows.getLong(7), rows.getLong(8), rows.getBytes(9));
    }
    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }
}