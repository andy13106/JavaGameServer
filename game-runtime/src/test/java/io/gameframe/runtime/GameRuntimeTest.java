package io.gameframe.runtime;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
class GameRuntimeTest {
    @Test void gridHandlesNegativeCoordinatesAndLeaving() {
        var grid = new GridAoi(10, 1);
        grid.move(1, -1, 0); grid.move(2, 0, 0); grid.move(3, 30, 0);
        assertEquals(Set.of(2L), grid.visible(1));
        grid.move(2, 30, 1); assertEquals(Set.of(), grid.visible(1));
        assertEquals(Set.of(3L), grid.visible(2)); grid.remove(3); assertTrue(grid.visible(2).isEmpty());
    }
    @Test void tickRecoversFromFullMailbox() throws Exception {
        try (var system = new ActorSystem(1, 1, 1, 1)) {
            var actor = system.spawn("room"); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            actor.tell(() -> { entered.countDown(); try { release.await(3, TimeUnit.SECONDS); } catch (InterruptedException e) { throw new RuntimeException(e); } });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            actor.tell(() -> {});
            CountDownLatch ticks = new CountDownLatch(2);
            try (var loop = new FixedStepLoop(actor, Duration.ofMillis(5), 2, nanos -> { actor.requireCurrent(); ticks.countDown(); })) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (loop.rejectedWakeups() == 0 && System.nanoTime() < deadline) Thread.sleep(5);
                assertTrue(loop.rejectedWakeups() > 0); release.countDown();
                assertTrue(ticks.await(2, TimeUnit.SECONDS));
            } finally { release.countDown(); }
        }
    }
}
