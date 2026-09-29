package io.gameframe.storage.mysql;

import io.gameframe.runtime.ServiceDirectorySnapshotApplier;
import io.gameframe.runtime.ServiceDirectorySnapshotTracker;
import io.gameframe.runtime.ServiceDirectoryView;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/** Connects a MySQL role watcher directly to the remote routing view. */
public final class MySqlServiceDirectoryBridge implements AutoCloseable {
    public record RefreshResult(boolean complete, ServiceDirectorySnapshotApplier.Result applied, int removed) {
        public RefreshResult {
            Objects.requireNonNull(applied, "applied");
            if (removed < 0) throw new IllegalArgumentException("removed must not be negative");
        }
    }

    private final int maxEntries;
    private final ServiceDirectoryView view;
    private final ServiceDirectorySnapshotTracker tracker;
    private final Set<String> completeRoles;
    private final MySqlServiceDirectoryWatcher watcher;
    private final AtomicReference<RefreshResult> last = new AtomicReference<>();

    public MySqlServiceDirectoryBridge(MySqlServiceLeaseRegistry registry, String role,
                                       ServiceDirectoryView view, ServiceDirectorySnapshotTracker tracker,
                                       int maxEntries, Duration interval) {
        Objects.requireNonNull(registry, "registry");
        this.view = Objects.requireNonNull(view, "view");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.completeRoles = Set.of(role);
        this.maxEntries = maxEntries;
        this.watcher = new MySqlServiceDirectoryWatcher(registry, role, maxEntries, interval,
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
                                              List<MySqlServiceLeaseRegistry.PublishedLease> snapshot,
                                              long nowMillis) {
        Objects.requireNonNull(snapshot, "snapshot");
        var observations = snapshot.stream().map(MySqlServiceLeaseRegistry.PublishedLease::observation).toList();
        boolean complete = snapshot.size() < maxEntries;
        if (complete) {
            var result = tracker.reconcile(view, observations, completeRoles, nowMillis);
            return new RefreshResult(true, result.applied(), result.removed());
        }
        return new RefreshResult(false, ServiceDirectorySnapshotApplier.apply(view, observations, nowMillis), 0);
    }

    @Override public void close() { watcher.close(); }
}