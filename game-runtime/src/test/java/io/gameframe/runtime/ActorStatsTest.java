package io.gameframe.runtime;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ActorStatsTest {
    @Test void statsExposeMailboxDepthProcessingFailuresAndLoadCandidates() throws Exception {
        try (var system = new ActorSystem(1, 3, 8, 1)) {
            var busy = system.spawn("busy");
            var idle = system.spawn("idle");
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var running = busy.tell(() -> { started.countDown(); try { release.await(3, TimeUnit.SECONDS); }
                catch (InterruptedException e) { throw new RuntimeException(e); } });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            var queued = busy.tell(() -> {});
            assertTrue(busy.stats().pending() >= 1);
            assertTrue(system.loadCandidates(1).getFirst().id().equals("idle"));
            release.countDown();
            running.toCompletableFuture().get(3, TimeUnit.SECONDS);
            queued.toCompletableFuture().get(3, TimeUnit.SECONDS);
            var failed = busy.tell(() -> { throw new IllegalStateException("expected"); });
            assertThrows(ExecutionException.class, () -> failed.toCompletableFuture().get(3, TimeUnit.SECONDS));
            var stats = busy.stats();
            assertEquals(3, stats.accepted());
            assertEquals(3, stats.processed());
            assertEquals(1, stats.failures());
            assertNotNull(stats.lastFailure());
            assertTrue(stats.totalProcessingNanos() > 0);
            assertTrue(stats.averageProcessingNanos() > 0);
            assertEquals(0, idle.stats().pending());
            assertEquals(2, system.stats().size());
        }
    }

    @Test void rejectedMessagesAreCounted() throws Exception {
        try (var system = new ActorSystem(1, 1, 1, 1)) {
            var actor = system.spawn("bounded");
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            actor.tell(() -> { entered.countDown(); try { release.await(3, TimeUnit.SECONDS); }
                catch (InterruptedException e) { throw new RuntimeException(e); } });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            actor.tell(() -> {});
            assertThrows(ExecutionException.class, () -> actor.tell(() -> {}).toCompletableFuture().get());
            assertEquals(1, actor.stats().rejected());
            release.countDown();
        }
    }
}
