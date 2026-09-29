package io.gameframe.runtime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

/** Bounded RPC response correlation with absolute deadlines and duplicate filtering. */
public final class RpcPendingCalls<T> implements AutoCloseable {
    public record Limits(int maxPending, int maxSettled) {
        public Limits {
            if (maxPending < 1 || maxSettled < 1) throw new IllegalArgumentException("RPC limits must be positive");
        }
        public static Limits defaults() { return new Limits(16_384, 16_384); }
    }

    public enum ReplyResult { COMPLETED, DUPLICATE, UNKNOWN, EXPIRED }

    public static final class RpcDeadlineException extends TimeoutException {
        public RpcDeadlineException(String requestId) { super("RPC deadline exceeded: " + requestId); }
    }

    private record Pending<T>(long deadlineMillis, CompletableFuture<T> future) {}

    private final Limits limits;
    private final Map<String, Pending<T>> pending = new HashMap<>();
    private final LinkedHashMap<String, Boolean> settled = new LinkedHashMap<>();
    private boolean closed;

    public RpcPendingCalls() { this(Limits.defaults()); }
    public RpcPendingCalls(Limits limits) { this.limits = Objects.requireNonNull(limits, "limits"); }

    public synchronized CompletionStage<T> register(String requestId, long deadlineMillis, long nowMillis) {
        requireId(requestId);
        requireNow(nowMillis);
        if (deadlineMillis <= nowMillis) throw new IllegalArgumentException("deadline must be in the future");
        if (closed) throw new IllegalStateException("RPC registry closed");
        if (pending.containsKey(requestId) || settled.containsKey(requestId))
            throw new IllegalArgumentException("requestId already used: " + requestId);
        if (pending.size() >= limits.maxPending()) throw new IllegalStateException("RPC pending capacity exceeded");
        var future = new CompletableFuture<T>();
        pending.put(requestId, new Pending<>(deadlineMillis, future));
        return future.minimalCompletionStage();
    }

    public synchronized ReplyResult complete(String requestId, T value, long nowMillis) {
        requireId(requestId);
        requireNow(nowMillis);
        Pending<T> call = pending.remove(requestId);
        if (call == null) return settled.containsKey(requestId) ? ReplyResult.DUPLICATE : ReplyResult.UNKNOWN;
        remember(requestId);
        if (nowMillis >= call.deadlineMillis()) {
            call.future().completeExceptionally(new RpcDeadlineException(requestId));
            return ReplyResult.EXPIRED;
        }
        call.future().complete(value);
        return ReplyResult.COMPLETED;
    }

    public synchronized ReplyResult fail(String requestId, Throwable cause, long nowMillis) {
        requireId(requestId);
        requireNow(nowMillis);
        Objects.requireNonNull(cause, "cause");
        Pending<T> call = pending.remove(requestId);
        if (call == null) return settled.containsKey(requestId) ? ReplyResult.DUPLICATE : ReplyResult.UNKNOWN;
        remember(requestId);
        if (nowMillis >= call.deadlineMillis()) {
            call.future().completeExceptionally(new RpcDeadlineException(requestId));
            return ReplyResult.EXPIRED;
        }
        call.future().completeExceptionally(cause);
        return ReplyResult.COMPLETED;
    }

    /** Called by the control-plane tick; complete and fail also reject late responses. */
    public synchronized int expire(long nowMillis) {
        requireNow(nowMillis);
        int count = 0;
        Iterator<Map.Entry<String, Pending<T>>> iterator = pending.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (nowMillis >= entry.getValue().deadlineMillis()) {
                iterator.remove();
                remember(entry.getKey());
                entry.getValue().future().completeExceptionally(new RpcDeadlineException(entry.getKey()));
                count++;
            }
        }
        return count;
    }

    public synchronized int pendingCount() { return pending.size(); }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        var failure = new IllegalStateException("RPC registry closed");
        for (var call : pending.values()) call.future().completeExceptionally(failure);
        pending.clear();
        settled.clear();
    }

    private void remember(String requestId) {
        settled.put(requestId, Boolean.TRUE);
        if (settled.size() > limits.maxSettled()) settled.remove(settled.keySet().iterator().next());
    }

    private static void requireId(String requestId) {
        if (requestId == null || requestId.isBlank()) throw new IllegalArgumentException("requestId required");
    }
    private static void requireNow(long nowMillis) {
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
    }
}