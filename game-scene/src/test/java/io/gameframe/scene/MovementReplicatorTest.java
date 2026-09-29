package io.gameframe.scene;

import io.gameframe.runtime.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class MovementReplicatorTest {
    private static void inActor(Consumer<ActorSystem.ActorRef> test) throws Exception {
        try (var system = new ActorSystem(2, 1, 32, 8)) {
            var actor = system.spawn("map");
            actor.tell(() -> test.accept(actor)).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
    private static GridAoi grid(ActorSystem.ActorRef actor) {
        var grid = new GridAoi(actor, 10, 1, 10);
        for (long id = 1; id <= 4; id++) grid.move(id, 0, 0);
        return grid;
    }
    private static MovementReplicator.Limits limits() {
        return new MovementReplicator.Limits(10, 100, 20, 100, 100);
    }
    @Test void lastStateWinsAndSharedEncodingIsProtectedFromMutatingSenders() throws Exception {
        inActor(actor -> {
            var received = new TreeMap<Long, Integer>();
            var movement = new MovementReplicator(actor, grid(actor), limits(), states -> {
                assertEquals(1, states.size());
                assertThrows(UnsupportedOperationException.class, () -> states.clear());
                byte[] copy = states.getFirst().payload(); copy[0] = 99;
                return states.getFirst().payload();
            }, (observer, packet) -> {
                received.put(observer, (int) packet[0]); packet[0] = 88; return true;
            });
            movement.markDirty(1, new byte[] {1});
            byte[] state = {2};
            assertEquals(MovementReplicator.MarkResult.REPLACED, movement.markDirty(1, state));
            state[0] = 3;
            var stats = movement.flush();
            assertEquals(Map.of(2L, 2, 3L, 2, 4L, 2), received);
            assertEquals(1, stats.encodedPackets());
            assertEquals(2, stats.reusedPackets());
            assertEquals(3, stats.acceptedPackets());
            assertEquals(0, movement.pendingStats().bytes());
            assertEquals(1, movement.pendingStats().merged());
        });
    }
    @Test void onlyCurrentObserversReceiveUpdatesAndRemovedEntitiesAreForgotten() throws Exception {
        inActor(actor -> {
            var aoi = grid(actor);
            var observers = new HashSet<Long>();
            var movement = new MovementReplicator(actor, aoi, limits(), states -> new byte[] {1},
                (observer, packet) -> observers.add(observer));
            movement.markDirty(1, new byte[] {1});
            aoi.move(2, 500, 500);
            aoi.remove(3);
            movement.forget(3);
            movement.flush();
            assertEquals(Set.of(4L), observers);
            movement.markDirty(1, new byte[] {2});
            aoi.remove(1);
            movement.forget(1);
            assertEquals(0, movement.pendingStats().entities());
            assertEquals(MovementReplicator.MarkResult.UNKNOWN_ENTITY, movement.markDirty(1, new byte[] {1}));
        });
    }
    @Test void dirtyCountBytesAndSinglePacketBudgetsAreExplicit() throws Exception {
        inActor(actor -> {
            var aoi = grid(actor);
            var limits = new MovementReplicator.Limits(1, 3, 3, 2, 100);
            var movement = new MovementReplicator(actor, aoi, limits, states -> new byte[3], (id, packet) -> fail("oversize"));
            assertEquals(MovementReplicator.MarkResult.ACCEPTED, movement.markDirty(1, new byte[3]));
            assertEquals(MovementReplicator.MarkResult.CAPACITY, movement.markDirty(2, new byte[1]));
            assertEquals(MovementReplicator.MarkResult.TOO_LARGE, movement.markDirty(1, new byte[4]));
            assertEquals(3, movement.pendingStats().bytes());
            assertEquals(3, movement.flush().rejectedPackets());
            var bytesLimited = new MovementReplicator(actor, aoi,
                new MovementReplicator.Limits(5, 3, 3, 10, 100), states -> new byte[1], (id, packet) -> true);
            bytesLimited.markDirty(1, new byte[3]);
            assertEquals(MovementReplicator.MarkResult.CAPACITY, bytesLimited.markDirty(2, new byte[1]));
            bytesLimited.markDirty(1, new byte[1]);
            assertEquals(MovementReplicator.MarkResult.ACCEPTED, bytesLimited.markDirty(2, new byte[2]));
            assertEquals(3, bytesLimited.pendingStats().bytes());
        });
    }
    @Test void visibilityLinkBudgetAbortsBeforeSendingAnUnboundedBatch() throws Exception {
        inActor(actor -> {
            var movement = new MovementReplicator(actor, grid(actor),
                new MovementReplicator.Limits(5, 10, 5, 10, 2), states -> fail("no encoding"),
                (id, packet) -> fail("no sending"));
            movement.markDirty(1, new byte[1]);
            assertTrue(movement.flush().visibilityBudgetExceeded());
            assertEquals(0, movement.pendingStats().entities());
        });
    }
    @Test void failuresAndRejectionsAreReportedAndNewMarksSurviveCallbacks() throws Exception {
        inActor(actor -> {
            var reference = new AtomicReference<MovementReplicator>();
            var movement = new MovementReplicator(actor, grid(actor), limits(), states -> new byte[] {1},
                (observer, packet) -> {
                    assertThrows(IllegalStateException.class, () -> reference.get().flush());
                    reference.get().markDirty(1, new byte[] {2});
                    if (observer == 2) throw new IllegalStateException("send failed");
                    return observer == 4;
                });
            reference.set(movement);
            movement.markDirty(1, new byte[] {1});
            var stats = movement.flush();
            assertEquals(1, stats.failures().size());
            assertEquals("send", stats.failures().getFirst().stage());
            assertEquals(1, stats.acceptedPackets());
            assertEquals(1, stats.rejectedPackets());
            assertEquals(1, movement.pendingStats().entities());
            assertEquals(1, movement.pendingStats().bytes());
        });
    }
    @Test void encodingFailureDoesNotLeakPendingStateOrHideTheError() throws Exception {
        inActor(actor -> {
            var movement = new MovementReplicator(actor, grid(actor), limits(),
                states -> { throw new IllegalArgumentException("encode"); }, (id, packet) -> fail("no sending"));
            movement.markDirty(1, new byte[] {1});
            var stats = movement.flush();
            assertEquals(3, stats.failures().size());
            assertTrue(stats.failures().stream().allMatch(error -> error.stage().equals("encode")));
            assertEquals(0, movement.pendingStats().bytes());
        });
    }
}
