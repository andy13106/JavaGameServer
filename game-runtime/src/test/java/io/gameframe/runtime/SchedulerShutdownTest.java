package io.gameframe.runtime;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class SchedulerShutdownTest {
    @Test void closeMarksEveryHandleAndRepeatedHandleCloseDoesNotUnderflow() throws Exception {
        var timer = new SharedTickScheduler(1, 100, "shared-shutdown");
        var handles = new ArrayList<SharedTickScheduler.Handle>();
        for (int i = 0; i < 100; i++) handles.add(timer.schedule(Duration.ofHours(1), () -> {}));
        timer.close();
        for (var h : handles) { assertTrue(h.isCancelled()); h.close(); h.close(); }
        assertEquals(0, timer.stats().activeTasks());
        assertTrue(timer.awaitTermination(Duration.ofSeconds(2)));
    }
    @Test void manyTimersShareBoundedThreadsAndCloseAllowsAlreadyRunningCallbackToFinish() throws Exception {
        var timer = new SharedTickScheduler(2, 32, "bounded-timers");
        var entered = new CountDownLatch(2); var release = new CountDownLatch(1);
        var threads = ConcurrentHashMap.<Thread>newKeySet();
        try {
            for (int i = 0; i < 32; i++) timer.schedule(Duration.ofMillis(1), () -> {
                threads.add(Thread.currentThread()); entered.countDown();
                try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { throw new AssertionError(e); }
            });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            timer.close(); release.countDown();
            assertTrue(timer.awaitTermination(Duration.ofSeconds(2)));
            assertEquals(2, threads.size()); assertEquals(0, timer.stats().activeTasks());
            assertEquals(0, timer.stats().failedTasks());
        } finally { release.countDown(); timer.close(); }
    }
    @Test void stoppingActorCancelsItsLoopAndFailureCancelsOnlyItsOwnTimer() throws Exception {
        try (var timer = new SharedTickScheduler(1, 8, "actor-loop"); var system = new ActorSystem(2, 2, 16, 2)) {
            var actor = system.spawn("room");
            var ticks = new CountDownLatch(1);
            try (var loop = new FixedStepLoop(actor, Duration.ofMillis(1), 1, n -> ticks.countDown(), timer)) {
                assertTrue(ticks.await(2, TimeUnit.SECONDS));
                actor.stop().toCompletableFuture().get(2, TimeUnit.SECONDS);
                // Completion dependents need not run before an external get() wakes.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (timer.stats().activeTasks() != 0 && System.nanoTime() < deadline) Thread.onSpinWait();
                assertEquals(0, timer.stats().activeTasks());
            }
            var bad = system.spawn("bad");
            var count = new AtomicInteger();
            try (var loop = new FixedStepLoop(bad, Duration.ofMillis(1), 1, n -> {
                count.incrementAndGet(); throw new IllegalStateException("simulation");
            }, timer)) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (loop.failure().isEmpty() && System.nanoTime() < deadline) Thread.onSpinWait();
                assertTrue(loop.failure().isPresent()); assertEquals(1, count.get());
            }
        }
    }
}
