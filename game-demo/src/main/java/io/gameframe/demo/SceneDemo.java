package io.gameframe.demo;

import io.gameframe.runtime.*;
import io.gameframe.scene.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Deterministic in-process scene sample; network protocol integration is a separate migration stage. */
public final class SceneDemo {
    public record Summary(int movementTicks, int passiveTicks, int visibilityChanges,
                          int firstBatchEncoded, int firstBatchReused, int acceptedPackets,
                          long mergedStates, double observer2Position, double observer3Position) {}
    private SceneDemo() {}

    public static Summary run() throws Exception {
        try (var actors = new ActorSystem(2, 1, 64, 16)) {
            var actor = actors.spawn("scene-demo");
            return actor.call(() -> simulate(actor)).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
    private static Summary simulate(ActorSystem.ActorRef actor) {
        var aoi = new GridAoi(actor, 10, 1, 128);
        var scheduler = new ActiveComponentScheduler(actor, 128, 32);
        var changes = new ArrayList<GridAoi.VisibilityChange>();
        var receivedPositions = new HashMap<Long, Double>();
        var movement = new MovementReplicator(actor, aoi,
            new MovementReplicator.Limits(128, 65_536, 64, 4096, 4096),
            states -> {
                var packet = ByteBuffer.allocate(4 + states.size() * 16).putInt(states.size());
                for (var state : states) packet.putLong(state.entityId()).put(state.payload());
                return packet.array();
            },
            (observer, packet) -> {
                var buffer = ByteBuffer.wrap(packet);
                int count = buffer.getInt();
                for (int i = 0; i < count; i++) {
                    long entity = buffer.getLong();
                    double x = buffer.getDouble();
                    if (entity == 1) receivedPositions.put(observer, x);
                }
                return true;
            });
        for (long id = 1; id <= 3; id++) changes.addAll(aoi.moveWithChanges(id, 0, 0));
        class Movement extends GameComponent {
            double x;
            int ticks;
            Movement() { super(256); updatePolicy(UpdatePolicy.everyFrame()); }
            @Override protected void onUpdate(long nanos) {
                ticks++; x += 2;
                changes.addAll(aoi.moveWithChanges(owner().id(), x, 0));
                var result = movement.markDirty(owner().id(), ByteBuffer.allocate(8).putDouble(x).array());
                if (result != MovementReplicator.MarkResult.ACCEPTED && result != MovementReplicator.MarkResult.REPLACED)
                    throw new IllegalStateException("movement refused: " + result);
            }
        }
        class Inventory extends GameComponent {
            int ticks;
            Inventory() { super(3 << 8); }
            @Override protected void onUpdate(long nanos) { ticks++; }
        }
        try (var player = new GameObject(1, scheduler);
             var observer2 = new GameObject(2, scheduler);
             var observer3 = new GameObject(3, scheduler)) {
            var mover = player.add(new Movement());
            var bag = player.add(new Inventory());
            long step = Duration.ofMillis(50).toNanos();
            for (int i = 0; i < 3; i++) requireHealthy(scheduler.update(step));
            var first = movement.flush();
            changes.addAll(aoi.moveWithChanges(3, 100, 0));
            requireHealthy(scheduler.update(step));
            var second = movement.flush();
            if (!first.failures().isEmpty() || !second.failures().isEmpty()
                || first.visibilityBudgetExceeded() || second.visibilityBudgetExceeded())
                throw new IllegalStateException("sample replication failed");
            return new Summary(mover.ticks, bag.ticks, changes.size(),
                first.encodedPackets(), first.reusedPackets(),
                first.acceptedPackets() + second.acceptedPackets(), movement.pendingStats().merged(),
                receivedPositions.get(2L), receivedPositions.get(3L));
        } finally {
            for (long id = 1; id <= 3; id++) {
                aoi.removeWithChanges(id);
                movement.forget(id);
            }
        }
    }
    private static void requireHealthy(ActiveComponentScheduler.Stats stats) {
        if (!stats.failures().isEmpty()) throw new IllegalStateException("component failed", stats.failures().getFirst().cause());
    }
}
