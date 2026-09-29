package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

class MigrationJournalTest {
    @Test
    void codecRoundTripPreservesMigrationAndBinarySnapshot() {
        var entry = entry(CrossProcessMigrationCoordinator.Phase.RECOVERY_REQUIRED, 3,
                new byte[]{0, 1, 2, (byte) 255});
        var decoded = MigrationJournal.decode(MigrationJournal.encode(entry));
        assertEquals(entry.migrationId(), decoded.migrationId());
        assertEquals(entry.actorId(), decoded.actorId());
        assertEquals(entry.phase(), decoded.phase());
        assertEquals(entry.version(), decoded.version());
        assertArrayEquals(entry.snapshot(), decoded.snapshot());
    }

    @Test
    void compareAndSetFencesStaleRecoveryWorkers() {
        var journal = new InMemoryJournal();
        var initial = entry(CrossProcessMigrationCoordinator.Phase.FREEZING, 1, new byte[]{7});
        assertTrue(journal.create(initial).toCompletableFuture().join());
        var prepared = entry(CrossProcessMigrationCoordinator.Phase.PREPARED, 2, new byte[]{8});
        assertTrue(journal.compareAndSet(initial.migrationId(), 1, prepared).toCompletableFuture().join());
        var stale = entry(CrossProcessMigrationCoordinator.Phase.ABORTING, 2, new byte[]{9});
        assertFalse(journal.compareAndSet(initial.migrationId(), 1, stale).toCompletableFuture().join());
        assertFalse(journal.delete(initial.migrationId(), 1).toCompletableFuture().join());
        assertTrue(journal.delete(initial.migrationId(), 2).toCompletableFuture().join());
        assertTrue(journal.load(initial.migrationId()).toCompletableFuture().join().isEmpty());
    }

    @Test
    void coordinatorPersistsAcceptedLifecycleAndDeletesJournal() throws Exception {
        var journal = new InMemoryJournal();
        var source = new CrossProcessMigrationCoordinator.Source() {
            public CompletionStage<Boolean> freeze(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(true); }
            public CompletionStage<Void> release(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(null); }
            public CompletionStage<Void> resume(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(null); }
        };
        var target = new CrossProcessMigrationCoordinator.Target() {
            public CompletionStage<Boolean> prepare(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(true); }
            public CompletionStage<Void> commit(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(null); }
            public CompletionStage<Void> cancel(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(null); }
        };
        var request = new CrossProcessMigrationCoordinator.Request("durable-accepted", "actor-1", "game-a", "game-b", 18,
                new byte[]{1, 2}, System.currentTimeMillis() + 5_000);
        try (var coordinator = new CrossProcessMigrationCoordinator(8, 1024, journal)) {
            var migration = coordinator.begin(request, source, target);
            assertTrue(migration.result().toCompletableFuture().get(1, java.util.concurrent.TimeUnit.SECONDS).accepted());
            assertTrue(journal.load(request.migrationId()).toCompletableFuture().join().isEmpty());
        }
    }

    @Test
    void coordinatorLeavesRecoveryRecordUntilAcknowledged() throws Exception {
        var journal = new InMemoryJournal();
        var source = new CrossProcessMigrationCoordinator.Source() {
            public CompletionStage<Boolean> freeze(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(true); }
            public CompletionStage<Void> release(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(null); }
            public CompletionStage<Void> resume(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(null); }
        };
        var target = new CrossProcessMigrationCoordinator.Target() {
            public CompletionStage<Boolean> prepare(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(true); }
            public CompletionStage<Void> commit(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.failedFuture(new IllegalStateException("commit lost")); }
            public CompletionStage<Void> cancel(CrossProcessMigrationCoordinator.Request request) { return CompletableFuture.completedFuture(null); }
        };
        var request = new CrossProcessMigrationCoordinator.Request("durable-recovery", "actor-2", "game-a", "game-b", 19,
                new byte[]{3, 4}, System.currentTimeMillis() + 5_000);
        try (var coordinator = new CrossProcessMigrationCoordinator(8, 1024, journal)) {
            var migration = coordinator.begin(request, source, target);
            var result = migration.result().toCompletableFuture().get(1, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(CrossProcessMigrationCoordinator.Outcome.RECOVERY_REQUIRED, result.outcome());
            var persisted = journal.load(request.migrationId()).toCompletableFuture().join();
            assertTrue(persisted.isPresent());
            assertEquals(CrossProcessMigrationCoordinator.Phase.RECOVERY_REQUIRED, persisted.get().phase());
            coordinator.acknowledgeRecovery(migration);
            assertTrue(journal.load(request.migrationId()).toCompletableFuture().join().isEmpty());
        }
    }
    private static MigrationJournal.Entry entry(CrossProcessMigrationCoordinator.Phase phase,
                                                 long version, byte[] snapshot) {
        return new MigrationJournal.Entry("migration-1", "actor-1", "game-a", "game-b",
                17, phase, version, 1234, snapshot);
    }

    private static final class InMemoryJournal implements MigrationJournal {
        private final java.util.Map<String, Entry> entries = new java.util.HashMap<>();
        @Override public synchronized CompletionStage<Boolean> create(Entry entry) {
            return CompletableFuture.completedFuture(entries.putIfAbsent(entry.migrationId(), entry) == null);
        }
        @Override public synchronized CompletionStage<Optional<Entry>> load(String id) {
            return CompletableFuture.completedFuture(Optional.ofNullable(entries.get(id)));
        }
        @Override public synchronized CompletionStage<Boolean> compareAndSet(String id, long expected, Entry replacement) {
            Entry current = entries.get(id);
            boolean changed = current != null && current.version() == expected
                    && replacement.version() == expected + 1;
            if (changed) entries.put(id, replacement);
            return CompletableFuture.completedFuture(changed);
        }
        @Override public synchronized CompletionStage<List<Entry>> list(CrossProcessMigrationCoordinator.Phase phase, int max) {
            return CompletableFuture.completedFuture(entries.values().stream().filter(e -> e.phase() == phase).limit(max).toList());
        }
        @Override public synchronized CompletionStage<Boolean> delete(String id, long expected) {
            Entry current = entries.get(id);
            if (current == null || current.version() != expected) return CompletableFuture.completedFuture(false);
            entries.remove(id); return CompletableFuture.completedFuture(true);
        }
    }
}