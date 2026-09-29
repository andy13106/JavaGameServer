package io.gameframe.storage;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class OrderedExecutorTest {
    @Test void rejectsBeforeExecutionAndDrainsAcceptedOperations() throws Exception {
        var executor = new OrderedExecutor("test", 1, 1);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        var first = executor.submit("p", () -> { entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return 1; });
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        var second = executor.submit("p", () -> 2);
        try {
            var error = assertThrows(ExecutionException.class, () -> executor.submit("p", () -> fail("rejected operation ran")).toCompletableFuture().get());
            assertEquals(StorageException.Outcome.NOT_EXECUTED, ((StorageException) error.getCause()).outcome());
            assertEquals(1, executor.rejectedCount());
        } finally { release.countDown(); }
        executor.close(); assertEquals(1, first.toCompletableFuture().get()); assertEquals(2, second.toCompletableFuture().get());
        assertThrows(ExecutionException.class, () -> executor.submit("p", () -> 3).toCompletableFuture().get());
    }
}
