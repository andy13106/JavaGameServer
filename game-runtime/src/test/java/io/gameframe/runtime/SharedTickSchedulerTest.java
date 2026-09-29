package io.gameframe.runtime;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class SharedTickSchedulerTest {
    @Test void boundedTasksCancelIdempotentlyAndShutdownRejectsNewWork() throws Exception {
        try (var scheduler = new SharedTickScheduler(1, 1, "test-shared-tick")) {
            var ticks = new CountDownLatch(2);
            var handle = scheduler.schedule(Duration.ofMillis(2), ticks::countDown);
            assertTrue(ticks.await(2, TimeUnit.SECONDS));
            assertEquals(1, scheduler.stats().activeTasks());
            assertThrows(RejectedExecutionException.class,
                () -> scheduler.schedule(Duration.ofMillis(2), () -> {}));
            assertEquals(1, scheduler.stats().rejectedRegistrations());
            handle.close(); handle.close();
            assertEquals(0, scheduler.stats().activeTasks());
        }
        var stopped = new SharedTickScheduler(1, 1, "test-stopped-tick");
        stopped.close(); stopped.close();
        assertThrows(RejectedExecutionException.class,
            () -> stopped.schedule(Duration.ofMillis(2), () -> {}));
        assertTrue(stopped.stats().closed());
    }

    @Test void taskFailureIsRecordedAndOnlyTheBrokenTaskIsCancelled() throws Exception {
        try (var scheduler = new SharedTickScheduler(1, 2, "test-failure-tick")) {
            var healthy = new CountDownLatch(2);
            var failingRuns = new AtomicInteger();
            scheduler.schedule(Duration.ofMillis(2), () -> {
                failingRuns.incrementAndGet(); throw new IllegalStateException("timer failure");
            });
            var healthyHandle = scheduler.schedule(Duration.ofMillis(2), healthy::countDown);
            assertTrue(healthy.await(2, TimeUnit.SECONDS));
            var stats = scheduler.stats();
            assertEquals(1, stats.failedTasks());
            assertNotNull(stats.lastFailure());
            assertEquals(1, stats.activeTasks());
            assertEquals(1, failingRuns.get());
            healthyHandle.close();
        }
    }

    @Test void fixedStepLoopCanUseAnExplicitSharedScheduler() throws Exception {
        try (var scheduler = new SharedTickScheduler(1, 4, "test-fixed-step-shared");
             var actors = new ActorSystem(1, 1, 16, 4)) {
            var actor = actors.spawn("room");
            var steps = new CountDownLatch(2);
            try (var loop = new FixedStepLoop(actor, Duration.ofMillis(2), 2,
                    ignored -> { actor.requireCurrent(); steps.countDown(); }, scheduler)) {
                assertTrue(steps.await(2, TimeUnit.SECONDS));
                assertTrue(scheduler.stats().firedTasks() > 0);
            }
            assertEquals(0, scheduler.stats().activeTasks());
        }
    }
}