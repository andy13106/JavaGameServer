package io.gameframe.runtime;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class ActorSystemTest {
    @Test void sameActorNeverRunsConcurrentlyAndPreservesOrder() throws Exception {
        try (var system = new ActorSystem(4, 10, 2048, 7)) {
            var actor = system.spawn("player"); var seen = new ArrayList<Integer>();
            AtomicInteger active = new AtomicInteger();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                int value = i;
                futures.add(actor.tell(() -> {
                    actor.requireCurrent(); assertEquals(1, active.incrementAndGet());
                    seen.add(value); active.decrementAndGet();
                }).toCompletableFuture());
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
            assertEquals(java.util.stream.IntStream.range(0, 1000).boxed().toList(), seen);
            assertThrows(IllegalStateException.class, actor::requireCurrent);
        }
    }
    @Test void boundedMailboxRejectsAndKeepsAcceptedMessages() throws Exception {
        try (var system = new ActorSystem(1, 1, 1, 1)) {
            var actor = system.spawn("player"); CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
            var running = actor.call(() -> { entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return 1; });
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            var queued = actor.call(() -> 2);
            try { assertThrows(ExecutionException.class, () -> actor.call(() -> 3).toCompletableFuture().get()); }
            finally { release.countDown(); }
            assertEquals(1, running.toCompletableFuture().get()); assertEquals(2, queued.toCompletableFuture().get());
        }
    }
    @Test void reservedCompletionReentersActorEvenWhenNormalCapacityIsExhausted() throws Exception {
        try (var system = new ActorSystem(2, 1, 1, 1)) {
            var actor = system.spawn("player"); var remote = new CompletableFuture<Integer>(); var handled = new CompletableFuture<Integer>();
            var delivered = actor.call(() -> actor.pipe(() -> remote, (value, error) -> {
                actor.requireCurrent(); assertNull(error); handled.complete(value);
            })).toCompletableFuture().get();
            assertThrows(ExecutionException.class, () -> actor.tell(() -> {}).toCompletableFuture().get());
            remote.complete(42);
            delivered.toCompletableFuture().get(3, TimeUnit.SECONDS); assertEquals(42, handled.get());
        }
    }
    @Test void closeFailsOutstandingExternalCallbacks() throws Exception {
        var system = new ActorSystem(1, 1, 2, 1); var actor = system.spawn("p");
        var remote = new CompletableFuture<Void>();
        var delivery = actor.call(() -> actor.pipe(() -> remote, (v, e) -> fail("must not execute"))).toCompletableFuture().get();
        system.close();
        assertThrows(ExecutionException.class, () -> delivery.toCompletableFuture().get());
        remote.complete(null);
        assertThrows(ExecutionException.class, () -> actor.tell(() -> {}).toCompletableFuture().get());
    }
    @Test void handlerFailureIsReportedAndDoesNotLoseFollowingCommands() throws Exception {
        try (var system = new ActorSystem(1, 1, 8, 4)) {
            var actor = system.spawn("p");
            var bad = actor.tell(() -> { throw new IllegalStateException("bad command"); });
            var good = actor.call(() -> 7);
            assertThrows(ExecutionException.class, () -> bad.toCompletableFuture().get());
            assertEquals(7, good.toCompletableFuture().get());
        }
    }
}
