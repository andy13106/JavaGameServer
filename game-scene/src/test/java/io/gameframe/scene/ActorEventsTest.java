package io.gameframe.scene;
import com.zfoo.event.anno.Bus;
import com.zfoo.event.enhance.IEventReceiver;
import com.zfoo.event.manager.EventBus;
import com.zfoo.event.model.IEvent;
import io.gameframe.runtime.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class ActorEventsTest {
    record Changed(int value) implements IEvent {}
    private static <T> T get(CompletionStage<T> stage) throws Exception { return stage.toCompletableFuture().get(3, TimeUnit.SECONDS); }
    @Test void deliveryRunsOnActorAndClosingComponentAndObjectRemovesSubscriptions() throws Exception {
        try (var bus = new ActorEvents(8); var actors = new ActorSystem(2, 1, 16, 2)) {
            var actor = actors.spawn("room"); var count = new AtomicInteger();
            var object = get(actor.call(() -> {
                var obj = new GameObject(1, new ActiveComponentScheduler(actor, 8, 4));
                obj.events(bus).subscribe(Changed.class, e -> { actor.requireCurrent(); count.addAndGet(e.value()); });
                obj.add(new GameComponent(256) {
                    protected void onAttach() { events(bus).subscribe(Changed.class, e -> { owner(); count.incrementAndGet(); }); }
                });
                return obj;
            }));
            EventBus.post(new Changed(10)); get(actor.tell(() -> {})); assertEquals(11, count.get());
            get(actor.tell(() -> object.remove(1)));
            EventBus.post(new Changed(10)); get(actor.tell(() -> {})); assertEquals(21, count.get());
            get(actor.tell(object::close)); assertEquals(0, bus.stats().subscriptions());
            EventBus.post(new Changed(10)); get(actor.tell(() -> {})); assertEquals(21, count.get());
        }
    }
    @Test void closingQueuedSubscriptionDiscardsDeliveryAndFullMailboxIsCounted() throws Exception {
        try (var bus = new ActorEvents(1); var actors = new ActorSystem(1, 1, 1, 1)) {
            var actor = actors.spawn("room"); var scope = bus.scope(actor);
            var subscription = get(actor.call(() -> scope.subscribe(Changed.class, e -> fail("closed or rejected"))));
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var running = actor.call(() -> { entered.countDown(); return release.await(3, TimeUnit.SECONDS); });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            try {
                bus.post(new Changed(1)); bus.post(new Changed(2));
                assertEquals(1, bus.stats().rejected());
                subscription.close(); subscription.close(); assertEquals(0, bus.stats().subscriptions());
            } finally { release.countDown(); }
            assertTrue(get(running));
            // stop drains the accepted queued event even though normal capacity is occupied.
            get(actor.stop()); assertEquals(1, bus.stats().discarded());
        }
    }
    @Test void failureAndCapacityAreExplicitAndAttachRollbackClosesScope() throws Exception {
        try (var bus = new ActorEvents(1); var actors = new ActorSystem(1, 1, 16, 2)) {
            var actor = actors.spawn("room");
            get(actor.tell(() -> {
                try (var object = new GameObject(1, new ActiveComponentScheduler(actor, 8, 4))) {
                    assertThrows(IllegalStateException.class, () -> object.add(new GameComponent(256) {
                        protected void onAttach() {
                            events(bus).subscribe(Changed.class, e -> {});
                            throw new IllegalStateException("attach");
                        }
                    }));
                    assertEquals(0, bus.stats().subscriptions());
                    object.events(bus).subscribe(Changed.class, e -> { throw new IllegalArgumentException("handler"); });
                    assertThrows(RejectedExecutionException.class, () -> object.events(bus).subscribe(Changed.class, e -> {}));
                    bus.post(new Changed(1)); // queued event will be discarded on object close
                }
            }));
            get(actor.tell(() -> {}));
            var scope = bus.scope(actor);
            get(actor.tell(() -> scope.subscribe(Changed.class, e -> { throw new IllegalArgumentException("handler"); })));
            bus.post(new Changed(1)); get(actor.tell(() -> {}));
            assertEquals(1, bus.stats().failed()); scope.close();
        }
    }
    @Test void unregisterUsesIdentityAndConcurrentRegisterPostRemovePreservesLiveReceiver() throws Exception {
        var calls = new AtomicInteger();
        class Receiver implements IEventReceiver {
            public Bus bus() { return Bus.CurrentThread; }
            public Object getBean() { return this; }
            public void invoke(IEvent event) { calls.incrementAndGet(); }
            public boolean equals(Object other) { return other instanceof Receiver; }
            public int hashCode() { return 1; }
        }
        var permanent = new Receiver(); EventBus.registerEventReceiver(Changed.class, permanent);
        try (var workers = Executors.newFixedThreadPool(4)) {
            var tasks = new ArrayList<Future<?>>();
            for (int t = 0; t < 4; t++) tasks.add(workers.submit(() -> {
                for (int i = 0; i < 100; i++) {
                    var temporary = new Receiver();
                    EventBus.registerEventReceiver(Changed.class, temporary); EventBus.post(new Changed(1));
                    assertTrue(EventBus.unregisterEventReceiver(Changed.class, temporary));
                }
            }));
            for (var task : tasks) task.get(5, TimeUnit.SECONDS);
            int before = calls.get(); EventBus.post(new Changed(1)); assertEquals(before + 1, calls.get());
        } finally { assertTrue(EventBus.unregisterEventReceiver(Changed.class, permanent)); }
        assertFalse(EventBus.unregisterEventReceiver(Changed.class, permanent));
    }
}
