package io.gameframe.storage.redis;

import io.gameframe.runtime.ServiceDirectorySnapshotApplier;
import io.gameframe.runtime.ServiceDirectorySnapshotTracker;
import io.gameframe.runtime.ServiceDirectoryView;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/** Connects Redis lease polling directly to the remote routing view. */
public final class RedisServiceDirectoryBridge implements AutoCloseable {
    public record RefreshResult(boolean complete,
                                ServiceDirectorySnapshotApplier.Result applied,
                                int removed) {
        public RefreshResult {
            Objects.requireNonNull(applied, "applied");
            if (removed < 0) throw new IllegalArgumentException("removed must not be negative");
        }
    }

    private final int maxEntries;
    private final ServiceDirectoryView view;
    private final ServiceDirectorySnapshotTracker tracker;
    private final Set<String> completeRoles;
    private final RedisServiceDirectoryWatcher watcher;
    private final AtomicReference<RefreshResult> last = new AtomicReference<>();

    public RedisServiceDirectoryBridge(RedisServiceLeaseRegistry registry,
                                       ServiceDirectoryView view,
                                       ServiceDirectorySnapshotTracker tracker,
                                       Set<String> completeRoles,
                                       int maxEntries,
                                       Duration interval) {
        Objects.requireNonNull(registry, "registry");
        this.view = Objects.requireNonNull(view, "view");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.completeRoles = Set.copyOf(Objects.requireNonNull(completeRoles, "completeRoles"));
        if (this.completeRoles.isEmpty()) throw new IllegalArgumentException("completeRoles required");
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
        this.maxEntries = maxEntries;
        this.watcher = new RedisServiceDirectoryWatcher(registry, maxEntries, interval,
                snapshot -> last.set(applySnapshot(view, tracker, this.completeRoles,
                        maxEntries, snapshot, System.currentTimeMillis())));
    }

    public CompletionStage<Boolean> start() { return watcher.start(); }
    public CompletionStage<Boolean> pollNow() { return watcher.pollNow(); }
    public RefreshResult lastResult() { return last.get(); }

    public static RefreshResult applySnapshot(ServiceDirectoryView view,
                                              ServiceDirectorySnapshotTracker tracker,
                                              Set<String> completeRoles,
                                              int maxEntries,
                                              List<RedisServiceLeaseRegistry.PublishedLease> snapshot,
                                              long nowMillis) {
        Objects.requireNonNull(snapshot, "snapshot");
        var observations = snapshot.stream().map(value -> value.observation(nowMillis)).toList();
        boolean complete = snapshot.size() < maxEntries;
        if (complete) {
            var result = tracker.reconcile(view, observations, completeRoles, nowMillis);
            return new RefreshResult(true, result.applied(), result.removed());
        }
        return new RefreshResult(false,
                ServiceDirectorySnapshotApplier.apply(view, observations, nowMillis), 0);
    }

    @Override public void close() { watcher.close(); }
}