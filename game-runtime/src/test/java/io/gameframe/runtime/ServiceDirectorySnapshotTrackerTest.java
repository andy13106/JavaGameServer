package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ServiceDirectorySnapshotTrackerTest {
    @Test
    void completeSnapshotRemovesMissingInstance() {
        var view = new ServiceDirectoryView();
        var tracker = new ServiceDirectorySnapshotTracker();
        long now = System.currentTimeMillis();
        var one = observation("one", 11, now);
        var two = observation("two", 22, now);
        var first = tracker.reconcile(view, List.of(one, two), Set.of("game"), now);
        assertEquals(2, first.applied().accepted());
        assertEquals(2, tracker.tracked());
        var second = tracker.reconcile(view, List.of(two), Set.of("game"), now + 1);
        assertEquals(1, second.removed());
        assertEquals(List.of("two"), view.snapshot("game", now + 1).stream()
                .map(ServiceDirectory.ServiceInstance::instanceId).toList());
    }

    @Test
    void filteredSnapshotDoesNotRemoveRolesNotDeclaredComplete() {
        var view = new ServiceDirectoryView();
        var tracker = new ServiceDirectorySnapshotTracker();
        long now = System.currentTimeMillis();
        var gate = observation("gate", "gate-1", 31, now);
        var game = observation("game", "game-1", 41, now);
        tracker.reconcile(view, List.of(gate, game), Set.of("gate", "game"), now);
        tracker.reconcile(view, List.of(game), Set.of("game"), now + 1);
        assertEquals(1, view.snapshot("gate", now + 1).size());
    }

    private static ServiceDirectoryView.Observation observation(String id, long token, long now) {
        return observation("game", id, token, now);
    }

    private static ServiceDirectoryView.Observation observation(String role, String id, long token, long now) {
        var registration = new ServiceDirectory.Registration(role, id, URI.create("http://127.0.0.1:19000"),
                Set.of("rpc"), 1, 60_000, 1);
        return new ServiceDirectoryView.Observation(registration,
                new ServiceDirectory.Lease(role, id, 1, token), now + 60_000, 0, false);
    }
}