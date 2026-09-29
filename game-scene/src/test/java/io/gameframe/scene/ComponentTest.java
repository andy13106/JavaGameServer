package io.gameframe.scene;

import io.gameframe.runtime.ActorSystem;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class ComponentTest {
    private static void inActor(Consumer<ActorSystem.ActorRef> test) throws Exception {
        try (var system = new ActorSystem(2, 2, 64, 8)) {
            var actor = system.spawn("map");
            actor.tell(() -> test.accept(actor)).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }
    private static class Probe extends GameComponent {
        final List<Long> elapsed = new ArrayList<>();
        int attaches, detaches;
        Probe(int type, UpdatePolicy policy) { super(type); updatePolicy(policy); }
        @Override protected void onAttach() { attaches++; }
        @Override protected void onDetach() { detaches++; }
        @Override protected void onUpdate(long nanos) { elapsed.add(nanos); }
    }
    @Test void identitySlotsAndActorOwnershipAreEnforced() throws Exception {
        try (var system = new ActorSystem(2, 2, 64, 8)) {
            var actor = system.spawn("map");
            var other = system.spawn("other-map");
            var object = actor.call(() -> new GameObject(1, new ActiveComponentScheduler(actor, 10, 10)))
                .toCompletableFuture().get(3, TimeUnit.SECONDS);
            var component = new Probe(256, UpdatePolicy.everyFrame());
            actor.tell(() -> {
                object.add(component);
                assertThrows(IllegalArgumentException.class, () -> object.add(new Probe(257, UpdatePolicy.passive())));
                assertSame(component, object.component(1).orElseThrow());
            }).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, object::components);
            assertThrows(IllegalStateException.class, () -> component.updatePolicy(UpdatePolicy.passive()));
            other.tell(() -> assertThrows(IllegalStateException.class, object::close))
                .toCompletableFuture().get(3, TimeUnit.SECONDS);
            actor.tell(() -> {
                assertTrue(object.remove(1));
                assertFalse(object.remove(1));
                object.close(); object.close();
                assertEquals(1, component.attaches);
                assertEquals(1, component.detaches);
            }).toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }
    @Test void failedAttachRollsBackAndCloseCleansRemainingComponentsAfterFailure() throws Exception {
        inActor(actor -> {
            var scheduler = new ActiveComponentScheduler(actor, 10, 10);
            var object = new GameObject(1, scheduler);
            var failedAttach = new Probe(256, UpdatePolicy.everyFrame()) {
                @Override protected void onAttach() { throw new IllegalStateException("attach"); }
            };
            assertThrows(IllegalStateException.class, () -> object.add(failedAttach));
            assertTrue(failedAttach.detached());
            assertEquals(1, failedAttach.detaches);
            assertTrue(object.components().isEmpty());
            assertEquals(0, scheduler.activeCount());
            var badClose = object.add(new Probe(256, UpdatePolicy.everyFrame()) {
                @Override protected void onDetach() { super.onDetach(); throw new IllegalStateException("detach"); }
            });
            var good = object.add(new Probe(512, UpdatePolicy.everyFrame()));
            assertThrows(IllegalStateException.class, object::close);
            assertTrue(badClose.detached());
            assertTrue(good.detached());
            assertEquals(1, good.detaches);
            assertEquals(0, scheduler.activeCount());
            object.close();
        });
    }
    @Test void fourPoliciesOnlyVisitActiveComponentsAndSlowFramesRunOnce() throws Exception {
        inActor(actor -> {
            var scheduler = new ActiveComponentScheduler(actor, 10, 10);
            var object = new GameObject(1, scheduler);
            var passive = object.add(new Probe(256, UpdatePolicy.passive()));
            var frame = object.add(new Probe(512, UpdatePolicy.everyFrame()));
            var ticks = object.add(new Probe(768, UpdatePolicy.everyTicks(2)));
            var interval = object.add(new Probe(1024, UpdatePolicy.interval(Duration.ofNanos(15))));
            assertEquals(3, scheduler.update(10).examined());
            assertEquals(3, scheduler.update(10).executed());
            assertEquals(List.of(10L, 10L), frame.elapsed);
            assertEquals(List.of(20L), ticks.elapsed);
            assertEquals(List.of(20L), interval.elapsed);
            scheduler.update(1_000);
            assertEquals(List.of(20L, 1_000L), interval.elapsed);
            assertTrue(passive.elapsed.isEmpty());
            passive.updatePolicy(UpdatePolicy.everyFrame());
            frame.updatePolicy(UpdatePolicy.passive());
            scheduler.update(1);
            assertEquals(List.of(1L), passive.elapsed);
            assertEquals(3, frame.elapsed.size());
            object.close();
        });
    }
    @Test void budgetRotatesFairlyAndDeferredComponentsReceiveAccumulatedTime() throws Exception {
        inActor(actor -> {
            var scheduler = new ActiveComponentScheduler(actor, 3, 1);
            var object = new GameObject(1, scheduler);
            var a = object.add(new Probe(256, UpdatePolicy.everyFrame()));
            var b = object.add(new Probe(512, UpdatePolicy.everyFrame()));
            var c = object.add(new Probe(768, UpdatePolicy.everyFrame()));
            for (int i = 0; i < 6; i++) {
                var stats = scheduler.update(10);
                assertEquals(1, stats.executed());
                assertEquals(2, stats.deferred());
            }
            assertEquals(List.of(10L, 30L), a.elapsed);
            assertEquals(List.of(20L, 30L), b.elapsed);
            assertEquals(List.of(30L, 30L), c.elapsed);
            object.close();
        });
    }
    @Test void capacityRefusalDoesNotLosePolicyOrLeaveAHalfAttachedComponent() throws Exception {
        inActor(actor -> {
            var scheduler = new ActiveComponentScheduler(actor, 1, 1);
            var object = new GameObject(1, scheduler);
            object.add(new Probe(256, UpdatePolicy.everyFrame()));
            var passive = object.add(new Probe(512, UpdatePolicy.passive()));
            assertThrows(RejectedExecutionException.class, () -> passive.updatePolicy(UpdatePolicy.everyFrame()));
            assertEquals(UpdatePolicy.passive(), passive.updatePolicy());
            var refused = new Probe(768, UpdatePolicy.everyFrame());
            assertThrows(RejectedExecutionException.class, () -> object.add(refused));
            assertTrue(refused.detached());
            assertTrue(object.component(3).isEmpty());
            assertEquals(1, scheduler.activeCount());
            object.remove(1);
            passive.updatePolicy(UpdatePolicy.everyFrame());
            scheduler.update(5);
            assertEquals(List.of(5L), passive.elapsed);
            object.close();
        });
    }
    @Test void updatesCanRemoveAndAddComponentsWithoutRunningNewComponentsInTheSameTick() throws Exception {
        inActor(actor -> {
            var scheduler = new ActiveComponentScheduler(actor, 5, 5);
            var object = new GameObject(1, scheduler);
            var fresh = new Probe(768, UpdatePolicy.everyFrame());
            object.add(new Probe(256, UpdatePolicy.everyFrame()) {
                @Override protected void onUpdate(long nanos) {
                    object.remove(2);
                    object.remove(1);
                    object.add(fresh);
                }
            });
            var removed = object.add(new Probe(512, UpdatePolicy.everyFrame()));
            assertEquals(1, scheduler.update(5).executed());
            assertTrue(removed.elapsed.isEmpty());
            assertTrue(fresh.elapsed.isEmpty());
            scheduler.update(7);
            assertEquals(List.of(7L), fresh.elapsed);
            object.close();
        });
    }
    @Test void failureIsReportedAndPausedWhileOtherComponentsContinue() throws Exception {
        inActor(actor -> {
            var scheduler = new ActiveComponentScheduler(actor, 5, 5);
            var object = new GameObject(1, scheduler);
            var bad = object.add(new Probe(256, UpdatePolicy.everyFrame()) {
                @Override protected void onUpdate(long nanos) { throw new IllegalStateException("logic"); }
            });
            var good = object.add(new Probe(512, UpdatePolicy.everyFrame()));
            var stats = scheduler.update(5);
            assertEquals(1, stats.failures().size());
            assertEquals(1, stats.failures().getFirst().entityId());
            assertEquals(256, stats.failures().getFirst().typeId());
            assertEquals(UpdatePolicy.passive(), bad.updatePolicy());
            scheduler.update(7);
            assertEquals(List.of(5L, 7L), good.elapsed);
            object.close();
        });
    }
    @Test void invalidIntervalsAndRecursiveUpdatesFailWithoutBreakingTheScheduler() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> UpdatePolicy.everyTicks(0));
        assertThrows(IllegalArgumentException.class, () -> UpdatePolicy.interval(Duration.ZERO));
        inActor(actor -> {
            var scheduler = new ActiveComponentScheduler(actor, 5, 5);
            var object = new GameObject(1, scheduler);
            object.add(new Probe(256, UpdatePolicy.everyFrame()) {
                @Override protected void onUpdate(long nanos) {
                    assertThrows(IllegalStateException.class, () -> scheduler.update(1));
                }
            });
            assertThrows(IllegalArgumentException.class, () -> scheduler.update(0));
            assertEquals(1, scheduler.update(1).executed());
            assertEquals(1, scheduler.update(1).executed());
            object.close();
        });
    }
}
