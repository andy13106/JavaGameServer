package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ServiceDirectoryTest {
    @Test
    void registersHeartbeatAndFiltersByRoleAndCapabilities() {
        var directory = new ServiceDirectory(new ServiceDirectory.Limits(4));
        var registration = registration("game", "game-1", Set.of("map:1", "combat"), 2, 100);

        assertEquals(ServiceDirectory.UpdateResult.REGISTERED, directory.register(registration, 1_000));
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED,
                directory.heartbeat("game", "game-1", 1_090));
        assertTrue(directory.choose("game", Set.of("combat"), "player-1", 1_100).isPresent());
        assertTrue(directory.choose("game", Set.of("map:2"), "player-1", 1_100).isEmpty());
        assertEquals(1, directory.snapshot("game", 1_100).size());
    }

    @Test
    void expiredMembersAreNeverRoutedAndCanBePurged() {
        var directory = new ServiceDirectory();
        directory.register(registration("gate", "gate-1", Set.of(), 1, 50), 100);

        assertTrue(directory.choose("gate", Set.of(), "client-1", 149).isPresent());
        assertTrue(directory.choose("gate", Set.of(), "client-1", 150).isEmpty());
        assertEquals(1, directory.purgeExpired(150));
        assertEquals(0, directory.size());
        assertEquals(ServiceDirectory.UpdateResult.NOT_FOUND,
                directory.heartbeat("gate", "gate-1", 151));
    }

    @Test
    void capacityRejectsNewInstancesButAllowsExistingRefresh() {
        var directory = new ServiceDirectory(new ServiceDirectory.Limits(1));
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED,
                directory.register(registration("game", "one", Set.of(), 1, 100), 0));
        assertEquals(ServiceDirectory.UpdateResult.CAPACITY_REJECTED,
                directory.register(registration("game", "two", Set.of(), 1, 100), 1));
        assertEquals(ServiceDirectory.UpdateResult.REGISTERED,
                directory.register(registration("game", "one", Set.of("new"), 2, 100), 2));
        assertEquals(2, directory.snapshot("game", 2).getFirst().weight());
    }

    @Test
    void drainingExcludesNewChoicesAndLoadChangesSelectionScore() {
        var directory = new ServiceDirectory();
        directory.register(registration("game", "game-a", Set.of("pvp"), 1, 1_000), 0);
        directory.register(registration("game", "game-b", Set.of("pvp"), 1, 1_000), 0);
        var first = directory.choose("game", Set.of("pvp"), "stable-key", 10).orElseThrow();
        assertEquals(first.instanceId(), directory.choose("game", Set.of("pvp"), "stable-key", 10).orElseThrow().instanceId());

        assertEquals(ServiceDirectory.UpdateResult.REGISTERED,
                directory.setDraining(first.role(), first.instanceId(), true, 10));
        var replacement = directory.choose("game", Set.of("pvp"), "stable-key", 10).orElseThrow();
        assertNotEquals(first.instanceId(), replacement.instanceId());
    }

    @Test
    void rejectsInvalidRegistrationAndTime() {
        assertThrows(IllegalArgumentException.class,
                () -> registration("", "id", Set.of(), 1, 1));
        var directory = new ServiceDirectory();
        assertThrows(IllegalArgumentException.class,
                () -> directory.purgeExpired(-1));
    }

    private static ServiceDirectory.Registration registration(String role, String id,
                                                               Set<String> capabilities,
                                                               int weight, long ttl) {
        return new ServiceDirectory.Registration(role, id,
                URI.create("http://127.0.0.1:9000"), capabilities, weight, ttl);
    }
}