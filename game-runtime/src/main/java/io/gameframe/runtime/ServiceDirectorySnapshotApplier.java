package io.gameframe.runtime;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** Applies bounded registry snapshots to the remote routing view with one result policy. */
public final class ServiceDirectorySnapshotApplier {
    private ServiceDirectorySnapshotApplier() {}

    public record Result(int total, Map<ServiceDirectoryView.UpdateResult, Integer> outcomes) {
        public Result {
            if (total < 0) throw new IllegalArgumentException("total must not be negative");
            outcomes = Map.copyOf(outcomes);
        }

        public int accepted() {
            return outcomes.getOrDefault(ServiceDirectoryView.UpdateResult.ADDED, 0)
                    + outcomes.getOrDefault(ServiceDirectoryView.UpdateResult.UPDATED, 0);
        }
    }

    public static Result apply(ServiceDirectoryView view,
                               Collection<ServiceDirectoryView.Observation> observations,
                               long nowMillis) {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(observations, "observations");
        var outcomes = new EnumMap<ServiceDirectoryView.UpdateResult, Integer>(ServiceDirectoryView.UpdateResult.class);
        int total = 0;
        for (var observation : observations) {
            var result = view.upsert(Objects.requireNonNull(observation, "observation"), nowMillis);
            outcomes.merge(result, 1, Integer::sum);
            total++;
        }
        return new Result(total, outcomes);
    }
}