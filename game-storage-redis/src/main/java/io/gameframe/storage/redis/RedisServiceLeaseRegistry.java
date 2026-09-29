package io.gameframe.storage.redis;

import io.gameframe.runtime.ServiceDirectory;
import io.gameframe.runtime.ServiceNodeController;
import io.gameframe.runtime.ServiceDirectoryView;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Redis-backed lease publication for a ServiceDirectory incarnation.
 * The local directory remains the routing authority; Redis supplies cross-process membership.
 */
public final class RedisServiceLeaseRegistry {
    private final RedisStore redis;
    private final String namespace;

    public RedisServiceLeaseRegistry(RedisStore redis) { this(redis, "gameframe:service"); }

    public RedisServiceLeaseRegistry(RedisStore redis, String namespace) {
        this.redis = Objects.requireNonNull(redis, "redis");
        if (namespace == null || namespace.isBlank()) throw new IllegalArgumentException("namespace required");
        this.namespace = namespace;
    }

    /** Adapts this registry to the common service-node lifecycle controller. */
    public ServiceNodeController.LeaseStore leaseStore() {
        return new ServiceNodeController.LeaseStore() {
            @Override public CompletionStage<Boolean> acquire(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease) {
                return RedisServiceLeaseRegistry.this.acquire(registration, lease);
            }
            @Override public CompletionStage<Boolean> renew(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease, long nowMillis) {
                return RedisServiceLeaseRegistry.this.renew(registration, lease, nowMillis);
            }
            @Override public CompletionStage<Boolean> update(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease,
                                                             long nowMillis, int load, boolean draining) {
                return RedisServiceLeaseRegistry.this.update(registration, lease, nowMillis, load, draining);
            }
            @Override public CompletionStage<Boolean> release(ServiceDirectory.Lease lease) {
                return RedisServiceLeaseRegistry.this.release(lease);
            }
        };
    }

    public CompletionStage<Boolean> acquire(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease) {
        Objects.requireNonNull(registration, "registration");
        Objects.requireNonNull(lease, "lease");
        validateLease(registration, lease);
        return redis.setIfAbsent(key(registration.role(), registration.instanceId()),
                encode(registration, lease, 0, false), Duration.ofMillis(registration.leaseTtlMillis()));
    }

    /** Backwards-compatible heartbeat with empty metadata. */
    public CompletionStage<Boolean> renew(ServiceDirectory.Registration registration,
                                          ServiceDirectory.Lease lease, long nowMillis) {
        return update(registration, lease, nowMillis, 0, false);
    }

    /** Refreshes the lease and publishes load/draining atomically with the value replacement. */
    public CompletionStage<Boolean> update(ServiceDirectory.Registration registration,
                                           ServiceDirectory.Lease lease, long nowMillis,
                                           int load, boolean draining) {
        Objects.requireNonNull(registration, "registration");
        Objects.requireNonNull(lease, "lease");
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
        if (load < 0) throw new IllegalArgumentException("load must not be negative");
        validateLease(registration, lease);
        String key = key(registration.role(), registration.instanceId());
        return redis.renewLease(key, ownerToken(lease), encode(registration, lease, load, draining),
                Duration.ofMillis(registration.leaseTtlMillis()));
    }

    public CompletionStage<Boolean> release(ServiceDirectory.Lease lease) {
        Objects.requireNonNull(lease, "lease");
        return redis.releaseLease(key(lease.role(), lease.instanceId()), ownerToken(lease));
    }

    public CompletionStage<String> read(String role, String instanceId) { return redis.get(key(role, instanceId)); }

    public record PublishedLease(ServiceDirectory.Registration registration,
                                 ServiceDirectory.Lease lease,
                                 int load,
                                 boolean draining) {
        public PublishedLease(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease) {
            this(registration, lease, 0, false);
        }
        public PublishedLease {
            Objects.requireNonNull(registration, "registration");
            Objects.requireNonNull(lease, "lease");
            if (load < 0) throw new IllegalArgumentException("load must not be negative");
        }
        public ServiceDirectoryView.Observation observation(long nowMillis) {
            if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
            long ttl = registration.leaseTtlMillis();
            long expiresAt = Long.MAX_VALUE - nowMillis < ttl ? Long.MAX_VALUE : nowMillis + ttl;
            return new ServiceDirectoryView.Observation(registration, lease, expiresAt, load, draining);
        }
    }

    /** Lists a bounded snapshot of live lease records published under this namespace. */
    public CompletionStage<List<PublishedLease>> list(int maxEntries) { return list(null, maxEntries); }

    /** Lists a bounded snapshot for one service role. Redis TTL expiry is the liveness filter. */
    public CompletionStage<List<PublishedLease>> list(String role, int maxEntries) {
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
        String pattern = role == null || role.isBlank() ? namespace + ":*" : namespace + ":" + encodePart(role) + ":*";
        return redis.scanKeys(pattern, maxEntries).thenCompose(keys -> {
            List<String> ordered = keys.stream().sorted().toList();
            List<CompletableFuture<String>> reads = ordered.stream().map(foundKey -> redis.get(foundKey).toCompletableFuture()).toList();
            return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).thenApply(ignored -> {
                var result = new java.util.ArrayList<PublishedLease>(reads.size());
                for (int i = 0; i < reads.size(); i++) {
                    String encoded = reads.get(i).join();
                    if (encoded == null) continue;
                    try { result.add(decode(ordered.get(i), encoded)); }
                    catch (RuntimeException ignoredRecord) { /* malformed or concurrently replaced */ }
                }
                return List.copyOf(result);
            });
        });
    }

    public String key(String role, String instanceId) {
        if (role == null || role.isBlank() || instanceId == null || instanceId.isBlank()) throw new IllegalArgumentException("role and instanceId required");
        return namespace + ":" + encodePart(role) + ":" + encodePart(instanceId);
    }

    private PublishedLease decode(String key, String encoded) {
        String prefix = namespace + ":";
        if (!key.startsWith(prefix)) throw new IllegalArgumentException("foreign lease key");
        String[] keyParts = key.substring(prefix.length()).split(":", 2);
        if (keyParts.length != 2) throw new IllegalArgumentException("invalid lease key");
        String role = decodePart(keyParts[0]);
        String instanceId = decodePart(keyParts[1]);
        String[] parts = encoded.split("\\|", -1);
        if (parts.length != 5 && parts.length != 7) throw new IllegalArgumentException("invalid lease record");
        String[] tokenParts = parts[0].split(":", 2);
        if (tokenParts.length != 2) throw new IllegalArgumentException("invalid lease token");
        long token = Long.parseUnsignedLong(tokenParts[0], 16);
        long generation = Long.parseLong(tokenParts[1]);
        URI endpoint = URI.create(decodePart(parts[1]));
        int weight = Integer.parseInt(parts[2]);
        long ttl = Long.parseLong(parts[3]);
        String capabilityText = decodePart(parts[4]);
        Set<String> capabilities = capabilityText.isBlank() ? Set.of() : Set.of(capabilityText.split(","));
        int load = parts.length == 7 ? Integer.parseInt(parts[5]) : 0;
        boolean draining = parts.length == 7 && Boolean.parseBoolean(parts[6]);
        var registration = new ServiceDirectory.Registration(role, instanceId, endpoint, capabilities, weight, ttl, generation);
        var lease = new ServiceDirectory.Lease(role, instanceId, generation, token);
        return new PublishedLease(registration, lease, load, draining);
    }

    private static void validateLease(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease) {
        if (!registration.role().equals(lease.role()) || !registration.instanceId().equals(lease.instanceId()) || registration.generation() != lease.generation()) {
            throw new IllegalArgumentException("registration and lease incarnation mismatch");
        }
    }
    private static String ownerToken(ServiceDirectory.Lease lease) { return token(lease) + "|"; }
    private static String token(ServiceDirectory.Lease lease) { return Long.toUnsignedString(lease.token(), 16) + ":" + lease.generation(); }
    private static String encode(ServiceDirectory.Registration registration, ServiceDirectory.Lease lease, int load, boolean draining) {
        return String.join("|", token(lease), encodePart(registration.endpoint().toString()), Integer.toString(registration.weight()),
                Long.toString(registration.leaseTtlMillis()), encodePart(String.join(",", registration.capabilities())),
                Integer.toString(load), Boolean.toString(draining));
    }
    private static String decodePart(String value) { return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8); }
    private static String encodePart(String value) { return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8)); }
}