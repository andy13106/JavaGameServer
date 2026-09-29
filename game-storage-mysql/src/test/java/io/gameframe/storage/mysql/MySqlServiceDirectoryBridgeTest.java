package io.gameframe.storage.mysql;

import io.gameframe.runtime.ServiceDirectory;
import io.gameframe.runtime.ServiceDirectorySnapshotTracker;
import io.gameframe.runtime.ServiceDirectoryView;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MySqlServiceDirectoryBridgeTest {
    @Test
    void completeRoleSnapshotRemovesMissingLease() {
        var view = new ServiceDirectoryView();
        var tracker = new ServiceDirectorySnapshotTracker();
        long now = System.currentTimeMillis();
        var first = MySqlServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 3,
                List.of(lease("one", 11, now), lease("two", 22, now)), now);
        assertTrue(first.complete());
        var second = MySqlServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 3,
                List.of(lease("two", 22, now + 1)), now + 1);
        assertEquals(1, second.removed());
        assertEquals("two", view.snapshot("game", now + 1).getFirst().instanceId());
    }

    @Test
    void cappedRoleSnapshotDoesNotDeleteUnseenLease() {
        var view = new ServiceDirectoryView();
        var tracker = new ServiceDirectorySnapshotTracker();
        long now = System.currentTimeMillis();
        MySqlServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 1,
                List.of(lease("one", 11, now)), now);
        var capped = MySqlServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 1,
                List.of(lease("two", 22, now + 1)), now + 1);
        assertFalse(capped.complete());
        assertEquals(2, view.snapshot("game", now + 1).size());
    }

    @Test
    void appliesPublishedLoadAndDrainingMetadata() {
        var view = new ServiceDirectoryView();
        var tracker = new ServiceDirectorySnapshotTracker();
        long now = System.currentTimeMillis();
        var registration = new ServiceDirectory.Registration("game", "busy",
                URI.create("http://127.0.0.1:19000"), Set.of("rpc"), 1, 60_000, 1);
        var published = new MySqlServiceLeaseRegistry.PublishedLease(registration,
                new ServiceDirectory.Lease("game", "busy", 1, 33), now + 60_000, 9, true);
        MySqlServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 3, List.of(published), now);
        var instance = view.snapshot("game", now + 1).getFirst();
        assertEquals(9, instance.load());
        assertTrue(instance.draining());
    }
    private static MySqlServiceLeaseRegistry.PublishedLease lease(String id, long token, long expiresAt) {
        var registration = new ServiceDirectory.Registration("game", id,
                URI.create("http://127.0.0.1:19000"), Set.of("rpc"), 1, 60_000, 1);
        return new MySqlServiceLeaseRegistry.PublishedLease(registration,
                new ServiceDirectory.Lease("game", id, 1, token), expiresAt + 60_000);
    }
}