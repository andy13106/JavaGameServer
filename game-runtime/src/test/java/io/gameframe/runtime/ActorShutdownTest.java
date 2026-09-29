package io.gameframe.runtime;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class ActorShutdownTest {
    @Test void manualStopDrainsStopPolicyAndAllowsSameIdReuse() throws Exception {
        try (var system = new ActorSystem(2, 1, 16, 1)) {
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var stops = new AtomicInteger();
            var actor = system.spawn("room", new ActorSystem.Lifecycle() {
                public void onStop(ActorSystem.ActorRef self) { self.requireCurrent(); stops.incrementAndGet(); }
            }, ActorSystem.FailurePolicy.STOP);
            var first = actor.call(() -> { entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return 1; });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var queued = actor.call(() -> 2);
            var stop = actor.stop(); actor.stop();
            release.countDown();
            assertEquals(1, first.toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertEquals(2, queued.toCompletableFuture().get(2, TimeUnit.SECONDS));
            stop.toCompletableFuture().get(2, TimeUnit.SECONDS);
            actor.stop().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(1, stops.get());
            var replacement = system.spawn("room");
            assertEquals(3, replacement.call(() -> 3).toCompletableFuture().get(2, TimeUnit.SECONDS));
        }
    }
    @Test void startupHasReservedLifecycleSlotAndFailureCannotRunCommands() throws Exception {
        try (var system = new ActorSystem(2, 2, 1, 1)) {
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var actor = system.spawn("room", new ActorSystem.Lifecycle() {
                public void onStart(ActorSystem.ActorRef self) {
                    entered.countDown();
                    try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
                    catch (InterruptedException e) { throw new RuntimeException(e); }
                    throw new IllegalStateException("startup");
                }
            }, ActorSystem.FailurePolicy.CONTINUE);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var command = actor.tell(() -> fail("startup failed"));
            assertEquals(1, actor.pending());
            release.countDown();
            assertThrows(ExecutionException.class, () -> command.toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> actor.started().toCompletableFuture().get(2, TimeUnit.SECONDS));
            actor.stopped().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(0, actor.stats().processed());
        }
    }
    @Test void completionCallbacksCanInspectSystemAndStopWithoutDeadlock() throws Exception {
        try (var system = new ActorSystem(4, 128, 8, 1)) {
            var futures = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < 100; i++) {
                var actor = system.spawn("a" + i);
                futures.add(actor.call(() -> 1).thenCompose(v -> {
                    system.stats(); system.loadCandidates(2); return actor.stop();
                }).toCompletableFuture());
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
            assertTrue(system.stats().isEmpty());
        }
    }
    @Test void concurrentProducersAndFrequentIdleTransitionsRemainSerial() throws Exception {
        try (var system = new ActorSystem(4, 1, 4096, 1);
             var producers = Executors.newFixedThreadPool(4)) {
            var actor = system.spawn("room"); var running = new AtomicInteger(); var count = new AtomicInteger();
            var writers = new ArrayList<Future<?>>();
            for (int p = 0; p < 4; p++) writers.add(producers.submit(() -> {
                for (int i = 0; i < 250; i++) actor.tell(() -> {
                    assertEquals(1, running.incrementAndGet()); count.incrementAndGet(); running.decrementAndGet();
                }).toCompletableFuture().join();
            }));
            for (var writer : writers) writer.get(5, TimeUnit.SECONDS);
            assertEquals(1000, count.get()); assertEquals(0, actor.stats().failures());
        }
    }
}
