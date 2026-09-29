package io.gameframe.storage.redis;

import io.gameframe.runtime.ServiceDirectory;
import io.gameframe.runtime.ServiceDirectorySnapshotTracker;
import io.gameframe.runtime.ServiceDirectoryView;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RedisServiceDirectoryBridgeTest {
    @Test
    void completeSnapshotRemovesMissingLease() {
        var view = new ServiceDirectoryView();
        var tracker = new ServiceDirectorySnapshotTracker();
        long now = System.currentTimeMillis();
        var first = RedisServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 3,
                List.of(lease("one", 11), lease("two", 22)), now);
        assertTrue(first.complete());
        assertEquals(2, first.applied().accepted());
        var second = RedisServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 3,
                List.of(lease("two", 22)), now + 1);
        assertEquals(1, second.removed());
        assertEquals("two", view.snapshot("game", now + 1).getFirst().instanceId());
    }

    @Test
    void cappedSnapshotNeverDeletesUnseenLease() {
        var view = new ServiceDirectoryView();
        var tracker = new ServiceDirectorySnapshotTracker();
        long now = System.currentTimeMillis();
        RedisServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 3,
                List.of(lease("one", 11), lease("two", 22)), now);
        var capped = RedisServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 1,
                List.of(lease("two", 22)), now + 1);
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
        var published = new RedisServiceLeaseRegistry.PublishedLease(registration,
                new ServiceDirectory.Lease("game", "busy", 1, 33), 9, true);
        RedisServiceDirectoryBridge.applySnapshot(view, tracker, Set.of("game"), 3, List.of(published), now);
        var instance = view.snapshot("game", now + 1).getFirst();
        assertEquals(9, instance.load());
        assertTrue(instance.draining());
    }
    private static RedisServiceLeaseRegistry.PublishedLease lease(String id, long token) {
        var registration = new ServiceDirectory.Registration("game", id,
                URI.create("http://127.0.0.1:19000"), Set.of("rpc"), 1, 60_000, 1);
        return new RedisServiceLeaseRegistry.PublishedLease(registration,
                new ServiceDirectory.Lease("game", id, 1, token));
    }
}