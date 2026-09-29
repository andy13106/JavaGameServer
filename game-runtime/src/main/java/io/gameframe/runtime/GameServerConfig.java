package io.gameframe.runtime;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/** Validated process-level settings shared by Gate, Game and control-plane services. */
public record GameServerConfig(String nodeId,
                               String serviceRole,
                               String bindHost,
                               int tcpPort,
                               int udpPort,
                               int maxConnections,
                               int tickRate,
                               Duration shutdownTimeout) {
    public GameServerConfig {
        requireText(nodeId, "nodeId");
        requireText(serviceRole, "serviceRole");
        requireText(bindHost, "bindHost");
        requirePort(tcpPort, "tcpPort");
        requirePort(udpPort, "udpPort");
        if (maxConnections < 1) throw new IllegalArgumentException("maxConnections must be positive");
        if (tickRate < 1 || tickRate > 1_000) throw new IllegalArgumentException("tickRate out of range");
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
        if (shutdownTimeout.isZero() || shutdownTimeout.isNegative() || shutdownTimeout.compareTo(Duration.ofMinutes(10)) > 0) {
            throw new IllegalArgumentException("shutdownTimeout out of range");
        }
    }

    public static GameServerConfig defaults(String nodeId, String serviceRole) {
        return new GameServerConfig(nodeId, serviceRole, "0.0.0.0", 0, 0, 10_000, 20, Duration.ofSeconds(30));
    }

    /** Reads optional string properties, retaining safe defaults for omitted keys. */
    public static GameServerConfig from(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");
        String nodeId = required(properties, "nodeId");
        String role = required(properties, "serviceRole");
        return new GameServerConfig(nodeId, role,
                value(properties, "bindHost", "0.0.0.0"),
                integer(properties, "tcpPort", 0), integer(properties, "udpPort", 0),
                integer(properties, "maxConnections", 10_000), integer(properties, "tickRate", 20),
                Duration.ofMillis(longValue(properties, "shutdownTimeoutMillis", 30_000)));
    }

    private static String required(Map<String, String> values, String key) {
        return requireText(values.get(key), key);
    }

    private static String value(Map<String, String> values, String key, String fallback) {
        String value = values.get(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int integer(Map<String, String> values, String key, int fallback) {
        String value = values.get(key);
        try { return value == null || value.isBlank() ? fallback : Integer.parseInt(value); }
        catch (NumberFormatException error) { throw new IllegalArgumentException(key + " must be an integer", error); }
    }

    private static long longValue(Map<String, String> values, String key, long fallback) {
        String value = values.get(key);
        try { return value == null || value.isBlank() ? fallback : Long.parseLong(value); }
        catch (NumberFormatException error) { throw new IllegalArgumentException(key + " must be a long", error); }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
        return value;
    }

    private static void requirePort(int value, String name) {
        if (value < 0 || value > 65_535) throw new IllegalArgumentException(name + " out of range");
    }
}