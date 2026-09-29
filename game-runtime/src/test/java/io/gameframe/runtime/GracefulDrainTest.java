package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class GracefulDrainTest {
    @Test
    void rejectsNewWorkAndWaitsForAdmittedWork() throws Exception {
        var drain = new GracefulDrain();
        var permit = drain.acquire();
        assertNotNull(permit);
        var drainingCalls = new AtomicInteger();
        var closed = new AtomicInteger();

        var shutdown = drain.shutdown(Duration.ofSeconds(2), drainingCalls::incrementAndGet,
                List.of(closed::incrementAndGet));
        assertEquals(GracefulDrain.State.DRAINING, drain.state());
        assertNull(drain.acquire());
        assertFalse(shutdown.toCompletableFuture().isDone());

        permit.close();
        var result = shutdown.toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertFalse(result.timedOut());
        assertEquals(0, result.unfinishedAtClose());
        assertEquals(1, drainingCalls.get());
        assertEquals(1, closed.get());
        assertEquals(GracefulDrain.State.TERMINATED, drain.state());
    }

    @Test
    void timeoutClosesInReverseOrderAndReportsUnfinishedWork() throws Exception {
        var drain = new GracefulDrain();
        var permit = drain.acquire();
        var order = new ArrayList<Integer>();
        var result = drain.shutdown(Duration.ofMillis(20), () -> {},
                        List.of(() -> order.add(1), () -> order.add(2)))
                .toCompletableFuture().get(2, TimeUnit.SECONDS);

        assertTrue(result.timedOut());
        assertEquals(1, result.unfinishedAtClose());
        assertEquals(List.of(2, 1), order);
        permit.close();
    }

    @Test
    void shutdownIsIdempotentAndCollectsCloseFailures() throws Exception {
        var drain = new GracefulDrain();
        var first = drain.shutdown(Duration.ZERO, () -> {},
                List.of(() -> { throw new IllegalStateException("close"); }));
        var second = drain.shutdown(Duration.ofSeconds(1), () -> fail("must not run"), List.of());

        assertSame(first, second);
        var result = first.toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(1, result.closeFailures().size());
        assertEquals("close", result.closeFailures().getFirst().getMessage());
    }
}
