package io.gameframe.runtime;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Bounded in-process idempotency gate for retryable RPC commands. */
public final class RpcIdempotencyRegistry<T> implements AutoCloseable {
    public record Limits(int maxEntries, long retentionMillis) {
        public Limits {
            if (maxEntries < 1 || retentionMillis < 1) throw new IllegalArgumentException("idempotency limits must be positive");
        }
        public static Limits defaults() { return new Limits(16_384, 60_000); }
    }

    public record Result<T>(CompletionStage<T> stage, boolean owner) {
        public Result { Objects.requireNonNull(stage, "stage"); }
    }

    private record Entry<T>(CompletableFuture<T> future, long retainedUntilMillis) {}
    private final Limits limits;
    private final LinkedHashMap<String, Entry<T>> entries = new LinkedHashMap<>();
    private boolean closed;

    public RpcIdempotencyRegistry() { this(Limits.defaults()); }
    public RpcIdempotencyRegistry(Limits limits) { this.limits = Objects.requireNonNull(limits, "limits"); }

    /** The supplier is called only for the owner; retries join the same future. */
    public synchronized Result<T> execute(String commandId, long nowMillis,
                                           Supplier<? extends CompletionStage<T>> operation) {
        requireId(commandId);
        requireNow(nowMillis);
        Objects.requireNonNull(operation, "operation");
        purgeLocked(nowMillis);
        if (closed) throw new IllegalStateException("idempotency registry closed");
        Entry<T> existing = entries.get(commandId);
        if (existing != null) return new Result<>(existing.future().minimalCompletionStage(), false);
        if (entries.size() >= limits.maxEntries()) throw new IllegalStateException("idempotency capacity exceeded");
        CompletableFuture<T> future = new CompletableFuture<>();
        entries.put(commandId, new Entry<>(future, safeAdd(nowMillis, limits.retentionMillis())));
        CompletionStage<T> stage;
        try {
            stage = Objects.requireNonNull(operation.get(), "operation returned null stage");
        } catch (Throwable error) {
            future.completeExceptionally(error);
            return new Result<>(future.minimalCompletionStage(), true);
        }
        stage.whenComplete((value, error) -> {
            if (error == null) future.complete(value);
            else future.completeExceptionally(error);
        });
        return new Result<>(future.minimalCompletionStage(), true);
    }

    public synchronized int purge(long nowMillis) {
        requireNow(nowMillis);
        return purgeLocked(nowMillis);
    }

    public synchronized int size() { return entries.size(); }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        var failure = new IllegalStateException("idempotency registry closed");
        for (Entry<T> entry : entries.values()) entry.future().completeExceptionally(failure);
        entries.clear();
    }

    private int purgeLocked(long nowMillis) {
        int removed = 0;
        Iterator<Map.Entry<String, Entry<T>>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            if (nowMillis >= iterator.next().getValue().retainedUntilMillis()) {
                iterator.remove();
                removed++;
            }
        }
        return removed;
    }

    private static long safeAdd(long nowMillis, long retentionMillis) {
        return Long.MAX_VALUE - nowMillis < retentionMillis ? Long.MAX_VALUE : nowMillis + retentionMillis;
    }
    private static void requireId(String commandId) {
        if (commandId == null || commandId.isBlank()) throw new IllegalArgumentException("commandId required");
    }
    private static void requireNow(long nowMillis) {
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
    }
}