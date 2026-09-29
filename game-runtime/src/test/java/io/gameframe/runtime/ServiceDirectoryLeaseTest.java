package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ServiceDirectoryLeaseTest {
    @Test
    void oldTokenCannotMutateAReincarnatedInstance() {
        var directory = new ServiceDirectory();
        var first = directory.registerLease(registration("game", "one", 1), 100).lease();

        assertEquals(ServiceDirectory.UpdateResult.FENCED,
                directory.heartbeat(new ServiceDirectory.Lease("game", "one", 1, first.token() + 1), 110));
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, directory.unregister(first, 110));
        assertEquals(ServiceDirectory.UpdateResult.STALE, directory.heartbeat(first, 111));

        var second = directory.registerLease(registration("game", "one", 2), 111).lease();
        assertNotEquals(first.token(), second.token());
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, directory.heartbeat(second, 112));
        assertEquals(ServiceDirectory.UpdateResult.STALE, directory.heartbeat(first, 112));
    }

    @Test
    void tombstoneRejectsLateSameGenerationRegistration() {
        var directory = new ServiceDirectory();
        var lease = directory.registerLease(registration("gate", "one", 7), 0).lease();
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, directory.unregister(lease, 1));

        assertEquals(ServiceDirectory.UpdateResult.STALE,
                directory.registerLease(registration("gate", "one", 7), 2).status());
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED,
                directory.registerLease(registration("gate", "one", 8), 2).status());
    }

    @Test
    void operatorDrainSurvivesReRegistrationUntilExplicitlyCleared() {
        var directory = new ServiceDirectory();
        var first = directory.registerLease(registration("map", "one", 1), 0).lease();
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, directory.setDraining(first, true, 1));
        assertTrue(directory.choose("map", Set.of(), "player", 1).isEmpty());
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, directory.unregister(first, 2));

        var second = directory.registerLease(registration("map", "one", 2), 3).lease();
        assertTrue(directory.choose("map", Set.of(), "player", 3).isEmpty());
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, directory.setDraining(second, false, 4));
        assertTrue(directory.choose("map", Set.of(), "player", 4).isPresent());
    }

    @Test
    void routeBindingIsRevokedAndRefencedAcrossIncarnations() {
        var directory = new ServiceDirectory();
        var first = directory.registerLease(registration("map", "one", 1), 0).lease();
        var binding = directory.assignRoute("player-1", first, 1).orElseThrow();
        assertSame(binding, directory.route("player-1", 1).orElseThrow());

        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, directory.unregister(first, 2));
        assertTrue(directory.route("player-1", 2).isEmpty());
        assertTrue(directory.assignRoute("player-1", first, 2).isEmpty());

        var second = directory.registerLease(registration("map", "one", 2), 3).lease();
        var replacement = directory.assignRoute("player-1", second, 3).orElseThrow();
        assertTrue(replacement.fencingToken() > binding.fencingToken());
    }

    @Test
    void expiryCreatesTombstoneAndSaturatingLeaseDeadline() {
        var directory = new ServiceDirectory();
        var lease = directory.registerLease(registration("gate", "one", 1, Long.MAX_VALUE), Long.MAX_VALUE - 2).lease();
        assertEquals(ServiceDirectory.UpdateResult.EXPIRED,
                directory.heartbeat(lease, Long.MAX_VALUE));
        assertEquals(ServiceDirectory.UpdateResult.STALE, directory.heartbeat(lease, Long.MAX_VALUE));
    }

    private static ServiceDirectory.Registration registration(String role, String id, long generation) {
        return registration(role, id, generation, 100);
    }

    private static ServiceDirectory.Registration registration(String role, String id,
                                                               long generation, long ttl) {
        return new ServiceDirectory.Registration(role, id,
                URI.create("http://127.0.0.1:9000"), Set.of("map"), 1, ttl, generation);
    }
}