package io.gameframe.transport;

import io.gameframe.runtime.RpcPendingCalls;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Correlates wire responses with bounded pending calls and rejects late/duplicate replies. */
public final class ZfooRpcPendingResponses implements AutoCloseable {
    public static final class RemoteCallException extends RuntimeException {
        public RemoteCallException(String commandId, String message) {
            super("RPC " + commandId + " failed: " + message);
        }
    }

    private final RpcPendingCalls<byte[]> pending;

    public ZfooRpcPendingResponses() {
        this(new RpcPendingCalls.Limits(16_384, 16_384));
    }

    public ZfooRpcPendingResponses(RpcPendingCalls.Limits limits) {
        this.pending = new RpcPendingCalls<>(Objects.requireNonNull(limits, "limits"));
    }

    public CompletionStage<byte[]> register(String commandId, long deadlineMillis, long nowMillis) {
        return pending.register(commandId, deadlineMillis, nowMillis);
    }

    public RpcPendingCalls.ReplyResult fail(String commandId, Throwable cause, long nowMillis) {
        return pending.fail(commandId, Objects.requireNonNull(cause, "cause"), nowMillis);
    }
    public RpcPendingCalls.ReplyResult accept(RpcResponseEnvelope response, long nowMillis) {
        Objects.requireNonNull(response, "response");
        if (response.isSuccess()) {
            return pending.complete(response.getCommandId(), response.getPayload(), nowMillis);
        }
        return pending.fail(response.getCommandId(),
                new RemoteCallException(response.getCommandId(), response.getError()), nowMillis);
    }

    public int expire(long nowMillis) {
        return pending.expire(nowMillis);
    }

    public int pendingCount() {
        return pending.pendingCount();
    }

    @Override
    public void close() {
        pending.close();
    }
}