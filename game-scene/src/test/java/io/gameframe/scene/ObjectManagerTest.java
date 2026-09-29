package io.gameframe.scene;
import io.gameframe.runtime.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class ObjectManagerTest {
    @Test void configuredConstructionHonorsCapacitySlotsAndActorOwnership() throws Exception {
        var factory = new ComponentFactory();
        class Health extends GameComponent {
            final int hp;
            Health(int hp) { super(256); this.hp = hp; }
        }
        assertTrue(factory.register(256, spec -> new Health(Integer.parseInt(spec.properties().get("hp")))));
        assertFalse(factory.register(256, spec -> new Health(0)));
        try (var actors = new ActorSystem(1, 1, 16, 2)) {
            var actor = actors.spawn("room");
            var manager = actor.call(() -> {
                var m = new ObjectManager(new ActiveComponentScheduler(actor, 8, 4), factory, 1);
                var obj = m.create(1, List.of(new ComponentFactory.Spec(256, Map.of("hp", "125"))));
                assertEquals(125, ((Health) obj.component(1).orElseThrow()).hp);
                assertThrows(IllegalArgumentException.class, () -> m.create(1, List.of()));
                assertThrows(RejectedExecutionException.class, () -> m.create(2, List.of()));
                assertTrue(m.remove(1)); assertFalse(m.remove(1));
                assertThrows(IllegalArgumentException.class, () -> m.create(2,
                    List.of(new ComponentFactory.Spec(256), new ComponentFactory.Spec(257))));
                assertEquals(0, m.size());
                return m;
            }).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, manager::size);
            actor.tell(manager::close).toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }
    @Test void failedConstructionRollsBackEarlierComponentsAndManagerCloseContinuesAfterError() throws Exception {
        var detached = new AtomicInteger(); var factory = new ComponentFactory();
        factory.register(256, spec -> new GameComponent(256) {
            protected void onDetach() { detached.incrementAndGet(); throw new IllegalStateException("detach"); }
        });
        factory.register(512, spec -> { throw new IllegalArgumentException("factory"); });
        try (var actors = new ActorSystem(1, 1, 16, 2)) {
            var actor = actors.spawn("room");
            actor.tell(() -> {
                var manager = new ObjectManager(new ActiveComponentScheduler(actor, 8, 4), factory, 4);
                var error = assertThrows(IllegalArgumentException.class, () -> manager.create(1,
                    List.of(new ComponentFactory.Spec(256), new ComponentFactory.Spec(512))));
                assertEquals(1, error.getSuppressed().length); assertEquals(0, manager.size());
                manager.create(2, List.of(new ComponentFactory.Spec(256)));
                manager.create(3, List.of(new ComponentFactory.Spec(256)));
                assertThrows(IllegalStateException.class, manager::close);
                assertEquals(0, manager.size()); assertEquals(3, detached.get()); manager.close();
            }).toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }
    @Test void wrongTypeAttachedReuseAndReentrantMutationAreRejected() throws Exception {
        var factory = new ComponentFactory();
        factory.register(256, spec -> new GameComponent(512) {});
        assertThrows(IllegalStateException.class, () -> factory.create(new ComponentFactory.Spec(256)));
        assertThrows(IllegalArgumentException.class, () -> factory.create(new ComponentFactory.Spec(768)));
        try (var actors = new ActorSystem(1, 1, 16, 2)) {
            var actor = actors.spawn("room");
            actor.tell(() -> {
                var localFactory = new ComponentFactory();
                var manager = new ObjectManager(new ActiveComponentScheduler(actor, 8, 4), localFactory, 4);
                localFactory.register(256, spec -> {
                    manager.create(2, List.of()); return new GameComponent(256) {};
                });
                assertThrows(IllegalStateException.class, () -> manager.create(1, List.of(new ComponentFactory.Spec(256))));
                assertEquals(0, manager.size());
                var obj = manager.create(3, List.of()); var component = obj.add(new GameComponent(512) {});
                localFactory.register(512, spec -> component);
                assertThrows(IllegalStateException.class, () -> manager.create(4, List.of(new ComponentFactory.Spec(512))));
                assertEquals(1, manager.size()); manager.close();
            }).toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }
}
