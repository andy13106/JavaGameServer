package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ServiceDirectoryViewTest {
    @Test
    void mergesRemoteInstancesAndChoosesByCapabilitiesAndLoad() {
        var view = new ServiceDirectoryView(new ServiceDirectoryView.Limits(2));
        var firstRegistration = registration("game", "one", 1, 2);
        var firstLease = new ServiceDirectory.Lease("game", "one", 1, 11);
        var secondRegistration = registration("game", "two", 1, 1);

        assertEquals(ServiceDirectoryView.UpdateResult.ADDED,
                view.upsert(firstRegistration, firstLease, 10_000, 0, false, 100));
        assertEquals(ServiceDirectoryView.UpdateResult.ADDED,
                view.upsert(secondRegistration, new ServiceDirectory.Lease("game", "two", 1, 22),
                        10_000, 5, false, 100));
        assertEquals("one", view.choose("game", Set.of("rpc"), "player-7", 100).orElseThrow().instanceId());
    }

    @Test
    void rejectsSameGenerationDifferentTokenAndOldGeneration() {
        var view = new ServiceDirectoryView();
        var registration = registration("map", "one", 3, 1);
        var lease = new ServiceDirectory.Lease("map", "one", 3, 33);
        assertEquals(ServiceDirectoryView.UpdateResult.ADDED,
                view.upsert(registration, lease, 100, 0, false, 1));
        assertEquals(ServiceDirectoryView.UpdateResult.FENCED,
                view.upsert(registration, new ServiceDirectory.Lease("map", "one", 3, 44),
                        100, 0, false, 2));
        assertEquals(ServiceDirectoryView.UpdateResult.UPDATED, view.remove(lease, 3));
        assertEquals(ServiceDirectoryView.UpdateResult.STALE,
                view.upsert(registration, lease, 100, 0, false, 4));
    }

    @Test
    void expiresAndRejectsNewEntriesAtCapacity() {
        var view = new ServiceDirectoryView(new ServiceDirectoryView.Limits(1));
        var registration = registration("gate", "one", 1, 1);
        var lease = new ServiceDirectory.Lease("gate", "one", 1, 55);
        assertEquals(ServiceDirectoryView.UpdateResult.ADDED,
                view.upsert(registration, lease, 10, 0, false, 1));
        assertEquals(ServiceDirectoryView.UpdateResult.EXPIRED,
                view.upsert(registration("gate", "two", 1, 1),
                        new ServiceDirectory.Lease("gate", "two", 1, 66), 10, 0, false, 10));
        assertEquals(1, view.purgeExpired(10));
        assertEquals(0, view.size());
    }

    private static ServiceDirectory.Registration registration(String role, String id, long generation, int weight) {
        return new ServiceDirectory.Registration(role, id, URI.create("http://127.0.0.1:9000"),
                Set.of("rpc"), weight, 1000, generation);
    }
}
