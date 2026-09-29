package io.gameframe.storage.mysql;

import io.gameframe.runtime.ServiceDirectory;
import io.gameframe.runtime.ServiceNodeController;
import io.gameframe.runtime.ServiceDirectoryView;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/** Explicit-SQL service lease registry for MySQL. */
public final class MySqlServiceLeaseRegistry {
    private final MySqlStore mysql;

    public MySqlServiceLeaseRegistry(MySqlStore mysql) { this.mysql = Objects.requireNonNull(mysql, "mysql"); }

    /** Creates the control-plane table and migrates metadata columns for existing installations. */
    public CompletionStage<Void> initializeSchema() {
        return mysql.execute("service-lease-schema", connection -> {
            try (var statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS game_service_lease (
                          role_name VARCHAR(128) NOT NULL,
                          instance_id VARCHAR(128) NOT NULL,
                          generation BIGINT NOT NULL,
                          lease_token BIGINT NOT NULL,
                          endpoint_text VARCHAR(768) NOT NULL,
                          capabilities_text TEXT NOT NULL,
                          weight INT NOT NULL,
                          lease_ttl_millis BIGINT NOT NULL,
                          expires_at_millis BIGINT NOT NULL,
                          load_count INT NOT NULL DEFAULT 0,
                          draining_flag BOOLEAN NOT NULL DEFAULT FALSE,
                          PRIMARY KEY (role_name, instance_id),
                          KEY idx_game_service_lease_role (role_name),
                          KEY idx_game_service_lease_expiry (expires_at_millis)
                        ) ENGINE=InnoDB
                        """);
                addColumnIfMissing(statement, "load_count INT NOT NULL DEFAULT 0");
                addColumnIfMissing(statement, "draining_flag BOOLEAN NOT NULL DEFAULT FALSE");
            }
            return null;
        });
    }

    public ServiceNodeController.LeaseStore leaseStore() {
        return new ServiceNodeController.LeaseStore() {
            @Override public CompletionStage<Boolean> acquire(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease) {
                return MySqlServiceLeaseRegistry.this.acquire(registration, lease, System.currentTimeMillis());
            }
            @Override public CompletionStage<Boolean> renew(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease, long nowMillis) {
                return MySqlServiceLeaseRegistry.this.renew(registration, lease, nowMillis);
            }
            @Override public CompletionStage<Boolean> update(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease,
                                                             long nowMillis, int load, boolean draining) {
                return MySqlServiceLeaseRegistry.this.update(registration, lease, nowMillis, load, draining);
            }
            @Override public CompletionStage<Boolean> release(ServiceDirectory.Lease lease) {
                return MySqlServiceLeaseRegistry.this.release(lease);
            }
        };
    }

    public CompletionStage<Boolean> acquire(ServiceDirectory.Registration registration,
                                            ServiceDirectory.Lease lease, long nowMillis) {
        Objects.requireNonNull(registration, "registration");
        Objects.requireNonNull(lease, "lease");
        requireNow(nowMillis);
        validateLease(registration, lease);
        return mysql.transaction(key(registration.role(), registration.instanceId()), connection -> {
            Row existing = readForUpdate(connection, registration.role(), registration.instanceId());
            if (existing != null && existing.expiresAtMillis() > nowMillis) return false;
            if (existing != null && registration.generation() <= existing.generation()) return false;
            if (existing == null) {
                try (var statement = connection.prepareStatement("""
                        INSERT INTO game_service_lease
                        (role_name, instance_id, generation, lease_token, endpoint_text,
                         capabilities_text, weight, lease_ttl_millis, expires_at_millis, load_count, draining_flag)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """)) {
                    bind(statement, registration, lease, expiry(nowMillis, registration.leaseTtlMillis()), 0, false);
                    statement.executeUpdate();
                    return true;
                }
            }
            try (var statement = connection.prepareStatement("""
                    UPDATE game_service_lease
                    SET generation=?, lease_token=?, endpoint_text=?, capabilities_text=?,
                        weight=?, lease_ttl_millis=?, expires_at_millis=?, load_count=0, draining_flag=FALSE
                    WHERE role_name=? AND instance_id=?
                    """)) {
                statement.setLong(1, registration.generation());
                statement.setLong(2, lease.token());
                statement.setString(3, registration.endpoint().toString());
                statement.setString(4, encodeCapabilities(registration.capabilities()));
                statement.setInt(5, registration.weight());
                statement.setLong(6, registration.leaseTtlMillis());
                statement.setLong(7, expiry(nowMillis, registration.leaseTtlMillis()));
                statement.setString(8, registration.role());
                statement.setString(9, registration.instanceId());
                return statement.executeUpdate() == 1;
            }
        });
    }

    public CompletionStage<Boolean> renew(ServiceDirectory.Registration registration,
                                          ServiceDirectory.Lease lease, long nowMillis) {
        return update(registration, lease, nowMillis, 0, false);
    }

    /** Refreshes exact incarnation and publishes load/draining metadata. */
    public CompletionStage<Boolean> update(ServiceDirectory.Registration registration,
                                           ServiceDirectory.Lease lease, long nowMillis,
                                           int load, boolean draining) {
        Objects.requireNonNull(registration, "registration");
        Objects.requireNonNull(lease, "lease");
        requireNow(nowMillis);
        if (load < 0) throw new IllegalArgumentException("load must not be negative");
        validateLease(registration, lease);
        return mysql.execute(key(registration.role(), registration.instanceId()), connection -> {
            try (var statement = connection.prepareStatement("""
                    UPDATE game_service_lease
                    SET endpoint_text=?, capabilities_text=?, weight=?, lease_ttl_millis=?,
                        expires_at_millis=?, load_count=?, draining_flag=?
                    WHERE role_name=? AND instance_id=? AND generation=? AND lease_token=?
                      AND expires_at_millis > ?
                    """)) {
                statement.setString(1, registration.endpoint().toString());
                statement.setString(2, encodeCapabilities(registration.capabilities()));
                statement.setInt(3, registration.weight());
                statement.setLong(4, registration.leaseTtlMillis());
                statement.setLong(5, expiry(nowMillis, registration.leaseTtlMillis()));
                statement.setInt(6, load);
                statement.setBoolean(7, draining);
                statement.setString(8, registration.role());
                statement.setString(9, registration.instanceId());
                statement.setLong(10, lease.generation());
                statement.setLong(11, lease.token());
                statement.setLong(12, nowMillis);
                return statement.executeUpdate() == 1;
            }
        });
    }

    public CompletionStage<Boolean> release(ServiceDirectory.Lease lease) {
        Objects.requireNonNull(lease, "lease");
        return mysql.execute(key(lease.role(), lease.instanceId()), connection -> {
            try (var statement = connection.prepareStatement("""
                    DELETE FROM game_service_lease
                    WHERE role_name=? AND instance_id=? AND generation=? AND lease_token=?
                    """)) {
                statement.setString(1, lease.role());
                statement.setString(2, lease.instanceId());
                statement.setLong(3, lease.generation());
                statement.setLong(4, lease.token());
                return statement.executeUpdate() == 1;
            }
        });
    }

    public CompletionStage<PublishedLease> read(String role, String instanceId) {
        requireText(role, "role"); requireText(instanceId, "instanceId");
        return mysql.execute(key(role, instanceId), connection -> {
            Row row;
            try (var statement = connection.prepareStatement("""
                    SELECT role_name, instance_id, generation, lease_token, endpoint_text,
                           capabilities_text, weight, lease_ttl_millis, expires_at_millis,
                           load_count, draining_flag
                    FROM game_service_lease WHERE role_name=? AND instance_id=?
                    """)) {
                statement.setString(1, role); statement.setString(2, instanceId);
                try (ResultSet result = statement.executeQuery()) { row = result.next() ? readRow(result) : null; }
            }
            return row == null ? null : row.toPublishedLease();
        });
    }

    public CompletionStage<List<PublishedLease>> list(int maxEntries) { return list(null, maxEntries); }

    public CompletionStage<List<PublishedLease>> list(String role, int maxEntries) {
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
        if (role != null && role.isBlank()) throw new IllegalArgumentException("role must not be blank");
        return mysql.execute(role == null ? "service-lease-list" : "service-lease-list:" + role, connection -> {
            String sql = role == null ? """
                    SELECT role_name, instance_id, generation, lease_token, endpoint_text,
                           capabilities_text, weight, lease_ttl_millis, expires_at_millis,
                           load_count, draining_flag
                    FROM game_service_lease ORDER BY role_name, instance_id LIMIT ?
                    """ : """
                    SELECT role_name, instance_id, generation, lease_token, endpoint_text,
                           capabilities_text, weight, lease_ttl_millis, expires_at_millis,
                           load_count, draining_flag
                    FROM game_service_lease WHERE role_name=? ORDER BY instance_id LIMIT ?
                    """;
            var result = new ArrayList<PublishedLease>();
            try (var statement = connection.prepareStatement(sql)) {
                if (role == null) statement.setInt(1, maxEntries); else { statement.setString(1, role); statement.setInt(2, maxEntries); }
                try (ResultSet rows = statement.executeQuery()) { while (rows.next()) result.add(readRow(rows).toPublishedLease()); }
            }
            return List.copyOf(result);
        });
    }

    public record PublishedLease(ServiceDirectory.Registration registration,
                                 ServiceDirectory.Lease lease,
                                 long expiresAtMillis,
                                 int load,
                                 boolean draining) {
        public PublishedLease(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease, long expiresAtMillis) {
            this(registration, lease, expiresAtMillis, 0, false);
        }
        public PublishedLease {
            Objects.requireNonNull(registration, "registration"); Objects.requireNonNull(lease, "lease");
            if (expiresAtMillis < 1) throw new IllegalArgumentException("expiresAtMillis must be positive");
            if (load < 0) throw new IllegalArgumentException("load must not be negative");
        }
        public ServiceDirectoryView.Observation observation() {
            return new ServiceDirectoryView.Observation(registration, lease, expiresAtMillis, load, draining);
        }
    }

    private static Row readForUpdate(java.sql.Connection connection, String role, String instanceId) throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT role_name, instance_id, generation, lease_token, endpoint_text,
                       capabilities_text, weight, lease_ttl_millis, expires_at_millis,
                       load_count, draining_flag
                FROM game_service_lease WHERE role_name=? AND instance_id=? FOR UPDATE
                """)) {
            statement.setString(1, role); statement.setString(2, instanceId);
            try (ResultSet result = statement.executeQuery()) { return result.next() ? readRow(result) : null; }
        }
    }

    private static Row readRow(ResultSet result) throws SQLException {
        return new Row(result.getString(1), result.getString(2), result.getLong(3), result.getLong(4),
                result.getString(5), result.getString(6), result.getInt(7), result.getLong(8),
                result.getLong(9), result.getInt(10), result.getBoolean(11));
    }

    private static void bind(java.sql.PreparedStatement statement, ServiceDirectory.Registration registration,
                             ServiceDirectory.Lease lease, long expiresAtMillis, int load, boolean draining) throws SQLException {
        statement.setString(1, registration.role()); statement.setString(2, registration.instanceId());
        statement.setLong(3, registration.generation()); statement.setLong(4, lease.token());
        statement.setString(5, registration.endpoint().toString()); statement.setString(6, encodeCapabilities(registration.capabilities()));
        statement.setInt(7, registration.weight()); statement.setLong(8, registration.leaseTtlMillis());
        statement.setLong(9, expiresAtMillis); statement.setInt(10, load); statement.setBoolean(11, draining);
    }

    private static void addColumnIfMissing(java.sql.Statement statement, String definition) throws SQLException {
        String name = definition.substring(0, definition.indexOf(' '));
        try { statement.execute("ALTER TABLE game_service_lease ADD COLUMN " + definition); }
        catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            if (!(e.getSQLState() != null && e.getSQLState().startsWith("42S21")) && !message.contains("duplicate column")) throw e;
        }
    }

    private static String key(String role, String instanceId) { return role + "/" + instanceId; }
    private static void validateLease(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease) {
        if (!registration.role().equals(lease.role()) || !registration.instanceId().equals(lease.instanceId()) || registration.generation() != lease.generation()) throw new IllegalArgumentException("registration and lease incarnation mismatch");
    }
    private static long expiry(long nowMillis, long ttlMillis) { return Long.MAX_VALUE - nowMillis < ttlMillis ? Long.MAX_VALUE : nowMillis + ttlMillis; }
    private static String encodeCapabilities(Set<String> capabilities) { String value = capabilities.stream().sorted().reduce((left, right) -> left + "\n" + right).orElse(""); return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8)); }
    private static Set<String> decodeCapabilities(String encoded) { String value = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8); return value.isBlank() ? Set.of() : Set.of(value.split("\\n", -1)); }
    private static void requireNow(long value) { if (value < 0) throw new IllegalArgumentException("nowMillis must not be negative"); }
    private static void requireText(String value, String name) { if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required"); }

    private record Row(String role, String instanceId, long generation, long token, String endpoint,
                       String capabilities, int weight, long ttl, long expiresAtMillis, int load, boolean draining) {
        private PublishedLease toPublishedLease() {
            var registration = new ServiceDirectory.Registration(role, instanceId, URI.create(endpoint), decodeCapabilities(capabilities), weight, ttl, generation);
            var lease = new ServiceDirectory.Lease(role, instanceId, generation, token);
            return new PublishedLease(registration, lease, expiresAtMillis, load, draining);
        }
    }
}