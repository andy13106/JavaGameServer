package io.gameframe.runtime;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Read-side view of service instances learned from a remote registry.
 *
 * <p>This view is deliberately separate from {@link ServiceDirectory}: local
 * registration remains authoritative for this process, while remote snapshots
 * can be merged here without allowing a stale snapshot to mutate local leases.</p>
 */
public final class ServiceDirectoryView {
    public record Limits(int maxInstances) {
        public Limits {
            if (maxInstances < 1) throw new IllegalArgumentException("maxInstances must be positive");
        }
        public static Limits defaults() { return new Limits(4_096); }
    }

    public enum UpdateResult { ADDED, UPDATED, STALE, FENCED, EXPIRED, CAPACITY_REJECTED }

    public record Observation(ServiceDirectory.Registration registration,
                              ServiceDirectory.Lease lease,
                              long expiresAtMillis,
                              int load,
                              boolean draining) {
        public Observation {
            Objects.requireNonNull(registration, "registration");
            Objects.requireNonNull(lease, "lease");
            if (expiresAtMillis < 1 || load < 0) throw new IllegalArgumentException("invalid observation");
        }
    }

    private record Key(String role, String instanceId) {}
    private final Limits limits;
    private final Map<Key, Entry> entries = new HashMap<>();
    private final Map<Key, Long> tombstones = new HashMap<>();
    private long revision;

    public ServiceDirectoryView() { this(Limits.defaults()); }

    public ServiceDirectoryView(Limits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    /** Merges one remote observation while preserving generation and token fencing. */
    public synchronized UpdateResult upsert(Observation observation, long nowMillis) {
        Objects.requireNonNull(observation, "observation");
        return upsert(observation.registration(), observation.lease(), observation.expiresAtMillis(),
                observation.load(), observation.draining(), nowMillis);
    }

    public synchronized UpdateResult upsert(ServiceDirectory.Registration registration,
                                             ServiceDirectory.Lease lease,
                                             long expiresAtMillis,
                                             int load,
                                             boolean draining,
                                             long nowMillis) {
        Objects.requireNonNull(registration, "registration");
        Objects.requireNonNull(lease, "lease");
        if (expiresAtMillis <= nowMillis || nowMillis < 0) return UpdateResult.EXPIRED;
        if (load < 0) throw new IllegalArgumentException("load must not be negative");
        if (!registration.role().equals(lease.role())
                || !registration.instanceId().equals(lease.instanceId())
                || registration.generation() != lease.generation()) {
            throw new IllegalArgumentException("registration and lease incarnation mismatch");
        }

        Key key = new Key(registration.role(), registration.instanceId());
        Entry previous = entries.get(key);
        long retired = tombstones.getOrDefault(key, 0L);
        if (registration.generation() <= retired) return UpdateResult.STALE;
        if (previous != null && registration.generation() < previous.lease.generation()) return UpdateResult.STALE;
        if (previous != null && registration.generation() == previous.lease.generation()
                && previous.lease.token() != lease.token()) return UpdateResult.FENCED;
        if (previous == null && entries.size() >= limits.maxInstances()) return UpdateResult.CAPACITY_REJECTED;

        boolean added = previous == null;
        entries.put(key, new Entry(registration, lease, expiresAtMillis, load, draining, ++revision));
        return added ? UpdateResult.ADDED : UpdateResult.UPDATED;
    }

    public synchronized UpdateResult remove(ServiceDirectory.Lease lease, long nowMillis) {
        Objects.requireNonNull(lease, "lease");
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
        Key key = new Key(lease.role(), lease.instanceId());
        Entry entry = entries.get(key);
        if (entry == null) return tombstones.getOrDefault(key, 0L) >= lease.generation()
                ? UpdateResult.STALE : UpdateResult.EXPIRED;
        if (entry.lease.generation() != lease.generation()) return UpdateResult.STALE;
        if (entry.lease.token() != lease.token()) return UpdateResult.FENCED;
        entries.remove(key);
        tombstones.merge(key, lease.generation(), Math::max);
        return UpdateResult.UPDATED;
    }

    public synchronized List<ServiceDirectory.ServiceInstance> snapshot(String role, long nowMillis) {
        if (role == null || role.isBlank()) throw new IllegalArgumentException("role required");
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
        purgeExpiredLocked(nowMillis);
        return entries.values().stream()
                .filter(entry -> entry.registration.role().equals(role))
                .map(Entry::snapshot)
                .sorted(Comparator.comparing(ServiceDirectory.ServiceInstance::instanceId))
                .toList();
    }

    public synchronized Optional<ServiceDirectory.ServiceInstance> choose(String role,
                                                                            Set<String> requiredCapabilities,
                                                                            String routingKey,
                                                                            long nowMillis) {
        if (role == null || role.isBlank() || routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("role and routingKey required");
        }
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
        Set<String> required = normalizeCapabilities(requiredCapabilities);
        purgeExpiredLocked(nowMillis);
        return entries.values().stream()
                .filter(entry -> entry.registration.role().equals(role))
                .filter(entry -> !entry.draining && entry.registration.capabilities().containsAll(required))
                .map(Entry::snapshot)
                .sorted(Comparator.comparingDouble((ServiceDirectory.ServiceInstance value) ->
                        score(value, routingKey)).reversed()
                        .thenComparing(ServiceDirectory.ServiceInstance::instanceId))
                .findFirst();
    }

    /** Chooses an instance while excluding targets temporarily marked unhealthy by a caller. */
    public synchronized Optional<ServiceDirectory.ServiceInstance> chooseExcluding(
            String role, Set<String> requiredCapabilities, String routingKey,
            Set<String> excludedInstanceIds, long nowMillis) {
        if (role == null || role.isBlank() || routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("role and routingKey required");
        }
        Objects.requireNonNull(excludedInstanceIds, "excludedInstanceIds");
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
        Set<String> required = normalizeCapabilities(requiredCapabilities);
        purgeExpiredLocked(nowMillis);
        return entries.values().stream()
                .filter(entry -> entry.registration.role().equals(role))
                .filter(entry -> !excludedInstanceIds.contains(entry.registration.instanceId()))
                .filter(entry -> !entry.draining && entry.registration.capabilities().containsAll(required))
                .map(Entry::snapshot)
                .sorted(Comparator.comparingDouble((ServiceDirectory.ServiceInstance value) ->
                        score(value, routingKey)).reversed()
                        .thenComparing(ServiceDirectory.ServiceInstance::instanceId))
                .findFirst();
    }
    public synchronized int purgeExpired(long nowMillis) {
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
        return purgeExpiredLocked(nowMillis);
    }

    public synchronized int size() { return entries.size(); }

    private int purgeExpiredLocked(long nowMillis) {
        var expired = entries.entrySet().stream()
                .filter(entry -> entry.getValue().expiresAtMillis <= nowMillis)
                .map(Map.Entry::getKey).toList();
        for (Key key : expired) {
            Entry entry = entries.remove(key);
            if (entry != null) tombstones.merge(key, entry.lease.generation(), Math::max);
        }
        return expired.size();
    }

    private static double score(ServiceDirectory.ServiceInstance instance, String routingKey) {
        long hash = mix64(routingKey.hashCode() * 0x9E3779B97F4A7C15L
                ^ instance.instanceId().hashCode());
        double stable = (hash >>> 1) / (double) Long.MAX_VALUE;
        return stable * instance.weight() / (1.0 + instance.load());
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
        return value ^ (value >>> 31);
    }

    private static Set<String> normalizeCapabilities(Set<String> capabilities) {
        Objects.requireNonNull(capabilities, "capabilities");
        var normalized = new HashSet<String>();
        for (String capability : capabilities) {
            if (capability == null || capability.isBlank()) throw new IllegalArgumentException("capability required");
            normalized.add(capability);
        }
        return Set.copyOf(normalized);
    }

    private record Entry(ServiceDirectory.Registration registration,
                         ServiceDirectory.Lease lease,
                         long expiresAtMillis,
                         int load,
                         boolean draining,
                         long revision) {
        private ServiceDirectory.ServiceInstance snapshot() {
            return new ServiceDirectory.ServiceInstance(registration.role(), registration.instanceId(),
                    registration.endpoint(), registration.capabilities(), registration.weight(), load,
                    expiresAtMillis, draining, revision);
        }
    }
}
