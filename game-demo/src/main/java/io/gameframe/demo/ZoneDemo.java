package io.gameframe.demo;

import com.zfoo.event.model.IEvent;
import io.gameframe.runtime.*;
import io.gameframe.scene.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Two actors, configured components, scoped zfoo events and an actual ownership handoff. */
public final class ZoneDemo {
    public record GoldChanged(long entityId, int delta) implements IEvent {}
    public record Summary(String outcome, int sourceObjects, int targetObjects, int gold, int subscriptionsAfterStop) {}
    private static final int PLAYER = 256;
    private static <T> T get(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    private static final class Player extends GameComponent {
        final ActorEvents bus;
        int gold; boolean frozen;
        Player(int gold, ActorEvents bus) { super(PLAYER); this.gold = gold; this.bus = bus; }
        protected void onAttach() {
            events(bus).subscribe(GoldChanged.class, event -> {
                if (event.entityId() == owner().id() && !frozen) gold += event.delta();
            });
        }
    }
    private static final class Zone implements ZoneTransferCoordinator.Zone {
        final ActorSystem.ActorRef actor;
        final ObjectManager objects;
        final Map<Long, ComponentFactory.Spec> staged = new HashMap<>();
        Zone(ActorSystem.ActorRef actor, ComponentFactory factory) {
            this.actor = actor;
            objects = new ObjectManager(new ActiveComponentScheduler(actor, 32, 8), factory, 32);
        }
        Player player(long id) { return (Player) objects.find(id).orElseThrow().component(1).orElseThrow(); }
        public void freeze(ZoneTransferCoordinator.TransferRequest r) {
            actor.requireCurrent(); var player = player(r.entityId());
            if (player.frozen) throw new IllegalStateException("already frozen");
            player.frozen = true;
        }
        public void release(ZoneTransferCoordinator.TransferRequest r) {
            actor.requireCurrent();
            if (!player(r.entityId()).frozen || !objects.remove(r.entityId())) throw new IllegalStateException("not frozen/owned");
        }
        public void resume(ZoneTransferCoordinator.TransferRequest r) { actor.requireCurrent(); player(r.entityId()).frozen = false; }
        public boolean prepare(ZoneTransferCoordinator.TransferRequest r) {
            actor.requireCurrent();
            if (objects.find(r.entityId()).isPresent() || r.payload().length != 4) return false;
            int gold = ByteBuffer.wrap(r.payload()).getInt();
            staged.put(r.transferId(), new ComponentFactory.Spec(PLAYER, Map.of("gold", Integer.toString(gold))));
            return true;
        }
        public void commit(ZoneTransferCoordinator.TransferRequest r) {
            actor.requireCurrent(); var spec = Objects.requireNonNull(staged.get(r.transferId()));
            objects.create(r.entityId(), List.of(spec)); staged.remove(r.transferId());
        }
        public void cancel(ZoneTransferCoordinator.TransferRequest r) { actor.requireCurrent(); staged.remove(r.transferId()); }
    }
    private static ActorSystem.ActorRef spawn(ActorSystem actors, String id, ComponentFactory factory, Zone[] holder) {
        return actors.spawn(id, new ActorSystem.Lifecycle() {
            public void onStart(ActorSystem.ActorRef actor) { holder[0] = new Zone(actor, factory); }
            public void onStop(ActorSystem.ActorRef actor) { if (holder[0] != null) holder[0].objects.close(); }
        }, ActorSystem.FailurePolicy.STOP);
    }
    public static Summary run() throws Exception {
        try (var ticks = new SharedTickScheduler(1, 16, "zone-demo-timer");
             var events = new ActorEvents(64);
             var actors = new ActorSystem(2, 2, 32, 8);
             var coordinator = new ZoneTransferCoordinator(ticks, 8)) {
            var factory = new ComponentFactory();
            factory.register(PLAYER, spec -> new Player(Integer.parseInt(spec.properties().get("gold")), events));
            Zone[] from = new Zone[1], to = new Zone[1];
            var source = spawn(actors, "source", factory, from); var target = spawn(actors, "target", factory, to);
            get(source.started()); get(target.started());
            coordinator.registerZone("source", source, from[0]); coordinator.registerZone("target", target, to[0]);
            get(source.tell(() -> from[0].objects.create(7, List.of(new ComponentFactory.Spec(PLAYER, Map.of("gold", "100"))))));
            events.post(new GoldChanged(7, 5));
            var transfer = get(source.call(() -> coordinator.begin("source", "target", 7,
                ByteBuffer.allocate(4).putInt(from[0].player(7).gold).array(), Duration.ofSeconds(2))));
            var result = get(transfer.result());
            events.post(new GoldChanged(7, 3));
            int gold = get(target.call(() -> to[0].player(7).gold));
            int sourceCount = get(source.call(from[0].objects::size));
            int targetCount = get(target.call(to[0].objects::size));
            get(source.stop()); get(target.stop());
            return new Summary(result.outcome().name(), sourceCount, targetCount, gold, events.stats().subscriptions());
        }
    }
}
