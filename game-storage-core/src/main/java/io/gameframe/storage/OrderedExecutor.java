package io.gameframe.storage;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/** Blocking drivers execute off actor threads. Equal keys share a bounded serial lane. */
public final class OrderedExecutor implements AutoCloseable {
    private final ThreadPoolExecutor[] lanes;
    private final LongAdder rejected = new LongAdder();
    private final ThreadLocal<Boolean> inside = ThreadLocal.withInitial(() -> false);
    public OrderedExecutor(String name, int laneCount, int queueCapacity) {
        if (laneCount < 1 || queueCapacity < 1) throw new IllegalArgumentException();
        lanes = new ThreadPoolExecutor[laneCount];
        for (int i = 0; i < laneCount; i++) {
            String threadName = name + "-" + i;
            lanes[i] = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(queueCapacity), r -> new Thread(r, threadName), new ThreadPoolExecutor.AbortPolicy());
        }
    }
    public <T> CompletionStage<T> submit(Object key, Callable<T> operation) {
        Objects.requireNonNull(key); Objects.requireNonNull(operation);
        CompletableFuture<T> result = new CompletableFuture<>();
        try {
            lanes[Math.floorMod(key.hashCode(), lanes.length)].execute(() -> {
                inside.set(true);
                try { result.complete(operation.call()); }
                catch (Throwable error) { result.completeExceptionally(error); }
                finally { inside.remove(); }
            });
        } catch (RejectedExecutionException error) {
            rejected.increment();
            result.completeExceptionally(new StorageException(StorageException.Outcome.NOT_EXECUTED, "storage queue full or closed", error));
        }
        return result.minimalCompletionStage();
    }
    public long rejectedCount() { return rejected.sum(); }
    public int queuedCount() { return Arrays.stream(lanes).mapToInt(e -> e.getQueue().size()).sum(); }
    /** Accepted jobs drain. Timeout does NOT mean their side effects were cancelled. */
    @Override public void close() {
        if (inside.get()) throw new IllegalStateException("cannot close storage from its callback");
        for (var lane : lanes) lane.shutdown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        for (var lane : lanes) {
            try {
                if (!lane.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                    throw new IllegalStateException("storage still draining; do not close its driver");
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        }
    }
}
