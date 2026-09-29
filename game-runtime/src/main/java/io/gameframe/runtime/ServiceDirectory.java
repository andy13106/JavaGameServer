package io.gameframe.runtime;

import java.net.URI;
import java.security.SecureRandom;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Bounded in-process service directory for the control plane.
 *
 * <p>Membership is protected by a lease. Every new incarnation receives a
 * generation and an opaque lease token; retired generations remain as
 * tombstones so an old process cannot revive itself with a late heartbeat.
 * Draining is an operator state and therefore survives a same-generation
 * registration refresh until it is explicitly cleared.</p>
 */
public final class ServiceDirectory {
    public record Limits(int maxInstances) {
        public Limits {
            if (maxInstances < 1) {
                throw new IllegalArgumentException("maxInstances must be positive");
            }
        }

        public static Limits defaults() {
            return new Limits(4_096);
        }
    }

    public record Registration(String role,
                               String instanceId,
                               URI endpoint,
                               Set<String> capabilities,
                               int weight,
                               long leaseTtlMillis,
                               long generation) {
        public Registration(String role,
                            String instanceId,
                            URI endpoint,
                            Set<String> capabilities,
                            int weight,
                            long leaseTtlMillis) {
            this(role, instanceId, endpoint, capabilities, weight, leaseTtlMillis, 1);
        }

        public Registration {
            requireText(role, "role");
            requireText(instanceId, "instanceId");
            Objects.requireNonNull(endpoint, "endpoint");
            if (endpoint.getScheme() == null || endpoint.getHost() == null) {
                throw new IllegalArgumentException("endpoint must have a scheme and host");
            }
            if (weight < 1 || leaseTtlMillis < 1 || generation < 1) {
                throw new IllegalArgumentException("weight, leaseTtlMillis and generation must be positive");
            }
            capabilities = normalizeCapabilities(capabilities);
        }
    }

    public record Lease(String role, String instanceId, long generation, long token) {
        public Lease {
            requireText(role, "role");
            requireText(instanceId, "instanceId");
            if (generation < 1 || token == 0) {
                throw new IllegalArgumentException("generation must be positive and token must be non-zero");
            }
        }
    }

    public record RegistrationResult(UpdateResult status, Lease lease) {
        public RegistrationResult {
            Objects.requireNonNull(status, "status");
            if (status == UpdateResult.REGISTERED && lease == null) {
                throw new IllegalArgumentException("registered result requires a lease");
            }
            if (status != UpdateResult.REGISTERED && lease != null) {
                throw new IllegalArgumentException("non-registered result cannot carry a lease");
            }
        }
    }

    public record RouteBinding(String routeKey, Lease lease, long fencingToken) {
        public RouteBinding {
            requireText(routeKey, "routeKey");
            Objects.requireNonNull(lease, "lease");
            if (fencingToken < 1) {
                throw new IllegalArgumentException("fencingToken must be positive");
            }
        }
    }

    public record ServiceInstance(String role,
                                  String instanceId,
                                  URI endpoint,
                                  Set<String> capabilities,
                                  int weight,
                                  int load,
                                  long leaseExpiresAtMillis,
                                  boolean draining,
                                  long revision) {
        public ServiceInstance {
            capabilities = Set.copyOf(capabilities);
            if (load < 0 || weight < 1 || leaseExpiresAtMillis < 1 || revision < 1) {
                throw new IllegalArgumentException("invalid service instance state");
            }
        }

        public boolean supports(Set<String> requiredCapabilities) {
            return capabilities.containsAll(requiredCapabilities);
        }
    }

    public enum UpdateResult {
        REGISTERED,
        CAPACITY_REJECTED,
        NOT_FOUND,
        EXPIRED,
        STALE,
        FENCED
    }

    private record Key(String role, String instanceId) {}

    private final Limits limits;
    private final SecureRandom random = new SecureRandom();
    private final Map<Key, Entry> entries = new HashMap<>();
    private final Map<Key, Long> tombstones = new HashMap<>();
    private final Set<Key> forcedDraining = new HashSet<>();
    private final Map<String, RouteBinding> routes = new HashMap<>();
    private long revision;
    private long fencingRevision;

    public ServiceDirectory() {
        this(Limits.defaults());
    }

    public ServiceDirectory(Limits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    public synchronized UpdateResult register(Registration registration, long nowMillis) {
        return registerLease(registration, nowMillis).status();
    }

    /** Registers an incarnation and returns the token required for future mutations. */
    public synchronized RegistrationResult registerLease(Registration registration, long nowMillis) {
        Objects.requireNonNull(registration, "registration");
        requireNow(nowMillis);
        purgeExpiredLocked(nowMillis);
        Key key = new Key(registration.role(), registration.instanceId());
        Entry existing = entries.get(key);
        long retiredGeneration = tombstones.getOrDefault(key, 0L);
        if (registration.generation() <= retiredGeneration && existing == null) {
            return new RegistrationResult(UpdateResult.STALE, null);
        }
        if (existing != null && registration.generation() < existing.registration.generation()) {
            return new RegistrationResult(UpdateResult.STALE, null);
        }
        if (existing == null && entries.size() >= limits.maxInstances()) {
            return new RegistrationResult(UpdateResult.CAPACITY_REJECTED, null);
        }

        boolean sameIncarnation = existing != null
                && existing.registration.generation() == registration.generation();
        long token = sameIncarnation ? existing.lease.token() : nextToken();
        boolean draining = forcedDraining.contains(key) || (sameIncarnation && existing.draining);
        int load = sameIncarnation ? existing.load : 0;
        long nextRevision = ++revision;
        Entry entry = new Entry(registration, new Lease(registration.role(), registration.instanceId(),
                registration.generation(), token), safeAdd(nowMillis, registration.leaseTtlMillis()),
                load, draining, nextRevision);
        entries.put(key, entry);
        return new RegistrationResult(UpdateResult.REGISTERED, entry.lease);
    }

    public synchronized UpdateResult heartbeat(String role, String instanceId, long nowMillis) {
        requireText(role, "role");
        requireText(instanceId, "instanceId");
        requireNow(nowMillis);
        Entry entry = entries.get(new Key(role, instanceId));
        if (entry == null) return UpdateResult.NOT_FOUND;
        if (entry.leaseExpiresAtMillis <= nowMillis) {
            retireLocked(new Key(role, instanceId), entry);
            return UpdateResult.EXPIRED;
        }
        entry.leaseExpiresAtMillis = safeAdd(nowMillis, entry.registration.leaseTtlMillis());
        entry.revision = ++revision;
        return UpdateResult.REGISTERED;
    }

    /** Renews a lease only when generation and token both match the live entry. */
    public synchronized UpdateResult heartbeat(Lease lease, long nowMillis) {
        requireNow(nowMillis);
        Objects.requireNonNull(lease, "lease");
        Key key = new Key(lease.role(), lease.instanceId());
        Entry current = entries.get(key);
        if (current != null && current.leaseExpiresAtMillis <= nowMillis
                && current.registration.generation() == lease.generation()
                && current.lease.token() == lease.token()) {
            retireLocked(key, current);
            return UpdateResult.EXPIRED;
        }
        Entry entry = activeEntryLocked(lease, nowMillis);
        if (entry == null) return classifyLeaseLocked(lease);
        entry.leaseExpiresAtMillis = safeAdd(nowMillis, entry.registration.leaseTtlMillis());
        entry.revision = ++revision;
        return UpdateResult.REGISTERED;
    }

    public synchronized UpdateResult reportLoad(String role, String instanceId, int load, long nowMillis) {
        requireText(role, "role");
        requireText(instanceId, "instanceId");
        requireNow(nowMillis);
        if (load < 0) throw new IllegalArgumentException("load must not be negative");
        Entry entry = entries.get(new Key(role, instanceId));
        if (entry == null) return UpdateResult.NOT_FOUND;
        if (entry.leaseExpiresAtMillis <= nowMillis) {
            retireLocked(new Key(role, instanceId), entry);
            return UpdateResult.EXPIRED;
        }
        entry.load = load;
        entry.revision = ++revision;
        return UpdateResult.REGISTERED;
    }

    public synchronized UpdateResult reportLoad(Lease lease, int load, long nowMillis) {
        requireNow(nowMillis);
        if (load < 0) throw new IllegalArgumentException("load must not be negative");
        Entry entry = activeEntryLocked(lease, nowMillis);
        if (entry == null) return classifyLeaseLocked(lease);
        entry.load = load;
        entry.revision = ++revision;
        return UpdateResult.REGISTERED;
    }

    public synchronized UpdateResult setDraining(String role, String instanceId, boolean draining, long nowMillis) {
        requireText(role, "role");
        requireText(instanceId, "instanceId");
        requireNow(nowMillis);
        Key key = new Key(role, instanceId);
        Entry entry = entries.get(key);
        if (entry == null) return UpdateResult.NOT_FOUND;
        if (entry.leaseExpiresAtMillis <= nowMillis) {
            retireLocked(key, entry);
            return UpdateResult.EXPIRED;
        }
        setDrainingLocked(key, entry, draining);
        return UpdateResult.REGISTERED;
    }

    public synchronized UpdateResult setDraining(Lease lease, boolean draining, long nowMillis) {
        requireNow(nowMillis);
        Entry entry = activeEntryLocked(lease, nowMillis);
        if (entry == null) return classifyLeaseLocked(lease);
        setDrainingLocked(new Key(lease.role(), lease.instanceId()), entry, draining);
        return UpdateResult.REGISTERED;
    }

    /** Removes an incarnation and leaves a tombstone fencing its generation. */
    public synchronized UpdateResult unregister(Lease lease, long nowMillis) {
        requireNow(nowMillis);
        Objects.requireNonNull(lease, "lease");
        Key key = new Key(lease.role(), lease.instanceId());
        Entry entry = entries.get(key);
        if (entry == null) return classifyLeaseLocked(lease);
        if (entry.registration.generation() != lease.generation()) return UpdateResult.STALE;
        if (entry.lease.token() != lease.token()) return UpdateResult.FENCED;
        retireLocked(key, entry);
        return UpdateResult.REGISTERED;
    }

    /** Assigns a route and returns its monotonic fencing token. */
    public synchronized Optional<RouteBinding> assignRoute(String routeKey, Lease lease, long nowMillis) {
        requireText(routeKey, "routeKey");
        requireNow(nowMillis);
        Entry entry = activeEntryLocked(lease, nowMillis);
        if (entry == null || entry.draining) return Optional.empty();
        RouteBinding binding = new RouteBinding(routeKey, lease, ++fencingRevision);
        routes.put(routeKey, binding);
        return Optional.of(binding);
    }

    /** Returns a route only while its exact lease is live and accepting work. */
    public synchronized Optional<RouteBinding> route(String routeKey, long nowMillis) {
        requireText(routeKey, "routeKey");
        requireNow(nowMillis);
        RouteBinding binding = routes.get(routeKey);
        if (binding == null) return Optional.empty();
        Entry entry = activeEntryLocked(binding.lease(), nowMillis);
        if (entry == null || entry.draining) {
            routes.remove(routeKey);
            return Optional.empty();
        }
        return Optional.of(binding);
    }

    public synchronized int revokeRoutes(String role, String instanceId, long nowMillis) {
        requireText(role, "role");
        requireText(instanceId, "instanceId");
        requireNow(nowMillis);
        return revokeRoutesLocked(new Key(role, instanceId));
    }

    public synchronized Optional<ServiceInstance> choose(String role,
                                                           Set<String> requiredCapabilities,
                                                           String routingKey,
                                                           long nowMillis) {
        requireText(role, "role");
        requireText(routingKey, "routingKey");
        requireNow(nowMillis);
        Set<String> required = normalizeCapabilities(requiredCapabilities);
        List<ServiceInstance> candidates = entries.values().stream()
                .filter(entry -> entry.registration.role().equals(role))
                .filter(entry -> entry.leaseExpiresAtMillis > nowMillis && !entry.draining)
                .map(Entry::snapshot)
                .filter(instance -> instance.supports(required))
                .sorted(Comparator.comparingDouble((ServiceInstance instance) ->
                        selectionScore(instance, routingKey)).reversed()
                        .thenComparing(ServiceInstance::instanceId))
                .toList();
        return candidates.isEmpty() ? Optional.empty() : Optional.of(candidates.getFirst());
    }

    public synchronized List<ServiceInstance> snapshot(String role, long nowMillis) {
        requireText(role, "role");
        requireNow(nowMillis);
        return entries.values().stream()
                .filter(entry -> entry.registration.role().equals(role))
                .map(Entry::snapshot)
                .sorted(Comparator.comparing(ServiceInstance::instanceId))
                .toList();
    }

    public synchronized int purgeExpired(long nowMillis) {
        requireNow(nowMillis);
        return purgeExpiredLocked(nowMillis);
    }

    public synchronized int size() {
        return entries.size();
    }

    private int purgeExpiredLocked(long nowMillis) {
        var expired = entries.entrySet().stream()
                .filter(entry -> entry.getValue().leaseExpiresAtMillis <= nowMillis)
                .map(Map.Entry::getKey)
                .toList();
        for (Key key : expired) {
            Entry entry = entries.get(key);
            if (entry != null) retireLocked(key, entry);
        }
        return expired.size();
    }

    private Entry activeEntryLocked(Lease lease, long nowMillis) {
        Objects.requireNonNull(lease, "lease");
        Key key = new Key(lease.role(), lease.instanceId());
        Entry entry = entries.get(key);
        if (entry == null) return null;
        if (entry.leaseExpiresAtMillis <= nowMillis) {
            retireLocked(key, entry);
            return null;
        }
        if (entry.registration.generation() != lease.generation() || entry.lease.token() != lease.token()) {
            return null;
        }
        return entry;
    }

    private UpdateResult classifyLeaseLocked(Lease lease) {
        Objects.requireNonNull(lease, "lease");
        Entry entry = entries.get(new Key(lease.role(), lease.instanceId()));
        if (entry == null) {
            return tombstones.getOrDefault(new Key(lease.role(), lease.instanceId()), 0L) >= lease.generation()
                    ? UpdateResult.STALE : UpdateResult.NOT_FOUND;
        }
        if (entry.registration.generation() != lease.generation()) return UpdateResult.STALE;
        return UpdateResult.FENCED;
    }

    private void setDrainingLocked(Key key, Entry entry, boolean draining) {
        entry.draining = draining;
        if (draining) {
            forcedDraining.add(key);
            revokeRoutesLocked(key);
        } else {
            forcedDraining.remove(key);
        }
        entry.revision = ++revision;
    }

    private void retireLocked(Key key, Entry entry) {
        entries.remove(key);
        tombstones.merge(key, entry.registration.generation(), Math::max);
        revokeRoutesLocked(key);
    }

    private int revokeRoutesLocked(Key key) {
        int removed = 0;
        var iterator = routes.entrySet().iterator();
        while (iterator.hasNext()) {
            var route = iterator.next();
            Lease lease = route.getValue().lease();
            if (lease.role().equals(key.role()) && lease.instanceId().equals(key.instanceId())) {
                iterator.remove();
                removed++;
            }
        }
        return removed;
    }

    private long nextToken() {
        long token;
        do {
            token = random.nextLong();
        } while (token == 0);
        return token;
    }

    private static long safeAdd(long nowMillis, long ttlMillis) {
        return Long.MAX_VALUE - nowMillis < ttlMillis ? Long.MAX_VALUE : nowMillis + ttlMillis;
    }

    private static double selectionScore(ServiceInstance instance, String routingKey) {
        long hash = mix64(routingKey.hashCode() * 0x9E3779B97F4A7C15L ^ instance.instanceId().hashCode());
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
            requireText(capability, "capability");
            normalized.add(capability);
        }
        return Set.copyOf(normalized);
    }

    private static void requireNow(long nowMillis) {
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }

    private static final class Entry {
        private final Registration registration;
        private final Lease lease;
        private long leaseExpiresAtMillis;
        private int load;
        private boolean draining;
        private long revision;

        private Entry(Registration registration, Lease lease, long leaseExpiresAtMillis, int load,
                       boolean draining, long revision) {
            this.registration = registration;
            this.lease = lease;
            this.leaseExpiresAtMillis = leaseExpiresAtMillis;
            this.load = load;
            this.draining = draining;
            this.revision = revision;
        }

        private ServiceInstance snapshot() {
            return new ServiceInstance(registration.role(), registration.instanceId(), registration.endpoint(),
                    registration.capabilities(), registration.weight(), load, leaseExpiresAtMillis,
                    draining, revision);
        }
    }
}