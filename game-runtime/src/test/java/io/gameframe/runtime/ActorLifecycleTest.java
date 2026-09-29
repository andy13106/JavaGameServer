package io.gameframe.runtime;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class ActorLifecycleTest {
    @Test void stopRunsHooksDrainsAcceptedWorkAndRemovesActor() throws Exception {
        try (var system = new ActorSystem(1, 2, 8, 1)) {
            var started = new CountDownLatch(1);
            var stopped = new CountDownLatch(1);
            var starts = new AtomicInteger();
            var seen = new AtomicInteger();
            var actor = system.spawn("room", new ActorSystem.Lifecycle() {
                public void onStart(ActorSystem.ActorRef self) { self.requireCurrent(); starts.incrementAndGet(); started.countDown(); }
                public void onStop(ActorSystem.ActorRef self) { self.requireCurrent(); stopped.countDown(); }
            }, ActorSystem.FailurePolicy.CONTINUE);
            assertTrue(started.await(2, TimeUnit.SECONDS));
            actor.tell(seen::incrementAndGet).toCompletableFuture().get(2, TimeUnit.SECONDS);
            actor.stop().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertTrue(stopped.await(2, TimeUnit.SECONDS));
            assertEquals(1, starts.get());
            assertEquals(1, seen.get());
            assertTrue(system.stats().isEmpty());
            assertThrows(ExecutionException.class, () -> actor.tell(() -> {}).toCompletableFuture().get());
        }
    }

    @Test void stopPolicyRejectsQueuedMessagesAfterFailureAndCallsFailureHook() throws Exception {
        try (var system = new ActorSystem(1, 2, 16, 1)) {
            var failureHook = new AtomicInteger();
            var actor = system.spawn("fragile", new ActorSystem.Lifecycle() {
                public void onFailure(ActorSystem.ActorRef self, Throwable error) {
                    self.requireCurrent(); failureHook.incrementAndGet();
                }
            }, ActorSystem.FailurePolicy.STOP);
            actor.tell(() -> { throw new IllegalStateException("boom"); });
            var queued = actor.tell(() -> fail("must be rejected"));
            assertThrows(ExecutionException.class, () -> queued.toCompletableFuture().get(2, TimeUnit.SECONDS));
            actor.stop().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(1, failureHook.get());
            assertTrue(actor.stats().stopping());
        }
    }
}
