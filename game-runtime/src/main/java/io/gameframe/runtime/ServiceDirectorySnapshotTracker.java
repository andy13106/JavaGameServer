package io.gameframe.runtime;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks complete registry snapshots and removes instances that disappear.
 * A caller must declare which roles are complete so a bounded or filtered
 * snapshot cannot accidentally delete unrelated membership.
 */
public final class ServiceDirectorySnapshotTracker {
    private record Key(String role, String instanceId) {}

    public record Result(ServiceDirectorySnapshotApplier.Result applied, int removed) {
        public Result {
            Objects.requireNonNull(applied, "applied");
            if (removed < 0) throw new IllegalArgumentException("removed must not be negative");
        }
    }

    private final Map<Key, ServiceDirectory.Lease> known = new HashMap<>();

    public synchronized Result reconcile(ServiceDirectoryView view,
                                         Collection<ServiceDirectoryView.Observation> observations,
                                         Set<String> completeRoles,
                                         long nowMillis) {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(observations, "observations");
        Objects.requireNonNull(completeRoles, "completeRoles");
        var normalizedRoles = new HashSet<String>();
        for (String role : completeRoles) {
            if (role == null || role.isBlank()) throw new IllegalArgumentException("complete role required");
            normalizedRoles.add(role);
        }
        var snapshot = java.util.List.copyOf(observations);
        var outcomes = new EnumMap<ServiceDirectoryView.UpdateResult, Integer>(ServiceDirectoryView.UpdateResult.class);
        var seen = new HashSet<Key>();
        for (var observation : snapshot) {
            var registration = observation.registration();
            var key = new Key(registration.role(), registration.instanceId());
            seen.add(key);
            var outcome = view.upsert(Objects.requireNonNull(observation, "observation"), nowMillis);
            outcomes.merge(outcome, 1, Integer::sum);
            if (outcome == ServiceDirectoryView.UpdateResult.ADDED
                    || outcome == ServiceDirectoryView.UpdateResult.UPDATED) {
                known.put(key, observation.lease());
            } else if (outcome == ServiceDirectoryView.UpdateResult.FENCED
                    || outcome == ServiceDirectoryView.UpdateResult.STALE) {
                known.putIfAbsent(key, observation.lease());
            }
        }
        int removed = 0;
        var missing = known.entrySet().stream()
                .filter(entry -> normalizedRoles.contains(entry.getKey().role()))
                .filter(entry -> !seen.contains(entry.getKey()))
                .toList();
        for (var entry : missing) {
            var outcome = view.remove(entry.getValue(), nowMillis);
            if (outcome == ServiceDirectoryView.UpdateResult.UPDATED
                    || outcome == ServiceDirectoryView.UpdateResult.EXPIRED
                    || outcome == ServiceDirectoryView.UpdateResult.STALE) {
                known.remove(entry.getKey(), entry.getValue());
                if (outcome == ServiceDirectoryView.UpdateResult.UPDATED) removed++;
            }
        }
        var applied = new ServiceDirectorySnapshotApplier.Result(snapshot.size(), outcomes);
        return new Result(applied, removed);
    }

    public synchronized int tracked() { return known.size(); }
}