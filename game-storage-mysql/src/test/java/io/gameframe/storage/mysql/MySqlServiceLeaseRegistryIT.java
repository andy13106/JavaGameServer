package io.gameframe.storage.mysql;

import io.gameframe.runtime.ServiceDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "GAME_TEST_MYSQL_URL", matches = ".+")
class MySqlServiceLeaseRegistryIT {
    @Test
    void publishesRenewsReincarnatesAndListsLease() throws Exception {
        var registration = new ServiceDirectory.Registration("game", "it-one",
                URI.create("http://127.0.0.1:19001"), Set.of("rpc", "map"), 2, 5_000, 1);
        var lease = new ServiceDirectory.Lease("game", "it-one", 1, 0x1122L);
        try (var store = new MySqlStore(System.getenv("GAME_TEST_MYSQL_URL"),
                System.getenv("GAME_TEST_MYSQL_USER"), System.getenv("GAME_TEST_MYSQL_PASSWORD"), 2, 32)) {
            var registry = new MySqlServiceLeaseRegistry(store);
            store.execute("service-lease-it-legacy-schema", connection -> {
                try (var statement = connection.createStatement()) {
                    statement.execute("DROP TABLE IF EXISTS game_service_lease");
                    statement.execute("""
                            CREATE TABLE game_service_lease (
                              role_name VARCHAR(128) NOT NULL, instance_id VARCHAR(128) NOT NULL,
                              generation BIGINT NOT NULL, lease_token BIGINT NOT NULL,
                              endpoint_text VARCHAR(768) NOT NULL, capabilities_text TEXT NOT NULL,
                              weight INT NOT NULL, lease_ttl_millis BIGINT NOT NULL, expires_at_millis BIGINT NOT NULL,
                              PRIMARY KEY (role_name, instance_id)
                            ) ENGINE=InnoDB
                            """);
                }
                return null;
            }).toCompletableFuture().get();
            registry.initializeSchema().toCompletableFuture().get();
            try {
                assertTrue(registry.acquire(registration, lease, 1_000).toCompletableFuture().get());
                assertFalse(registry.acquire(registration, lease, 1_001).toCompletableFuture().get());
                assertTrue(registry.renew(registration, lease, 1_002).toCompletableFuture().get());
                assertTrue(registry.update(registration, lease, 1_003, 7, true).toCompletableFuture().get());
                var found = registry.read("game", "it-one").toCompletableFuture().get();
                assertNotNull(found);
                assertEquals(lease, found.lease());
                assertEquals(7, found.load());
                assertTrue(found.draining());
                assertEquals(1, registry.list("game", 10).toCompletableFuture().get().size());

                var replacementRegistration = new ServiceDirectory.Registration("game", "it-one",
                        URI.create("http://127.0.0.1:19002"), Set.of("rpc"), 1, 5_000, 2);
                var replacementLease = new ServiceDirectory.Lease("game", "it-one", 2, 0x3344L);
                assertTrue(registry.acquire(replacementRegistration, replacementLease, Long.MAX_VALUE - 1)
                        .toCompletableFuture().get());
                assertFalse(registry.release(lease).toCompletableFuture().get());
                assertTrue(registry.release(replacementLease).toCompletableFuture().get());
            } finally {
                store.execute("service-lease-it-cleanup", connection -> {
                    try (var statement = connection.createStatement()) {
                        statement.execute("DROP TABLE IF EXISTS game_service_lease");
                    }
                    return null;
                }).toCompletableFuture().get();
            }
        }
    }

    @Test
    void migrationJournalUsesVersionFencingAndRecoveryListing() throws Exception {
        try (var store = new MySqlStore(System.getenv("GAME_TEST_MYSQL_URL"),
                System.getenv("GAME_TEST_MYSQL_USER"), System.getenv("GAME_TEST_MYSQL_PASSWORD"), 2, 32)) {
            var journal = new MySqlMigrationJournal(store, 1024);
            journal.initializeSchema().toCompletableFuture().get();
            String id = "migration-it-" + java.util.UUID.randomUUID();
            var initial = new io.gameframe.runtime.MigrationJournal.Entry(id, "actor-1", "game-a", "game-b", 7,
                    io.gameframe.runtime.CrossProcessMigrationCoordinator.Phase.PREPARING, 1,
                    System.currentTimeMillis(), new byte[]{1, 2, 3});
            var recovery = new io.gameframe.runtime.MigrationJournal.Entry(id, "actor-1", "game-a", "game-b", 7,
                    io.gameframe.runtime.CrossProcessMigrationCoordinator.Phase.RECOVERY_REQUIRED, 2,
                    System.currentTimeMillis(), new byte[]{4, 5});
            try {
                assertTrue(journal.create(initial).toCompletableFuture().get());
                assertTrue(journal.compareAndSet(id, 1, recovery).toCompletableFuture().get());
                var stale = new io.gameframe.runtime.MigrationJournal.Entry(id, "actor-1", "game-a", "game-b", 7,
                        io.gameframe.runtime.CrossProcessMigrationCoordinator.Phase.PREPARING, 2,
                        System.currentTimeMillis(), new byte[]{9});
                assertFalse(journal.compareAndSet(id, 1, stale).toCompletableFuture().get());
                assertEquals(1, journal.list(io.gameframe.runtime.CrossProcessMigrationCoordinator.Phase.RECOVERY_REQUIRED, 10)
                        .toCompletableFuture().get().size());
                assertTrue(journal.delete(id, 2).toCompletableFuture().get());
                assertTrue(journal.load(id).toCompletableFuture().get().isEmpty());
            } finally {
                store.execute("migration-journal-it-cleanup", connection -> {
                    try (var statement = connection.prepareStatement("DELETE FROM game_migration_journal WHERE migration_id=?")) {
                        statement.setString(1, id); statement.executeUpdate();
                    }
                    return null;
                }).toCompletableFuture().get();
            }
        }
    }}
