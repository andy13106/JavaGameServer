package io.gameframe.transport;

import com.zfoo.net.session.Session;
import io.gameframe.runtime.RpcCallExecutor;
import io.gameframe.runtime.RpcPendingCalls;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/** Sends zfoo RPC envelopes over an existing Session and correlates typed responses. */
public final class ZfooRpcClientAdapter implements AutoCloseable {
    @FunctionalInterface
    public interface Sender {
        CompletionStage<?> send(Session session, RpcRequestEnvelope request, Object attachment);
    }

    private final Session session;
    private final Object attachment;
    private final Sender sender;
    private final RpcCallExecutor executor;
    private final ZfooRpcPendingResponses pending;
    private final ConcurrentHashMap<String, java.util.function.Function<byte[], ?>> decoders = new ConcurrentHashMap<>();

    public ZfooRpcClientAdapter(Session session, Object attachment, ZfooSender sender) {
        this(session, attachment, (current, request, routeAttachment) -> sender.send(current, request, routeAttachment));
    }

    public ZfooRpcClientAdapter(Session session, Object attachment, Sender sender) {
        this(session, attachment, sender, new RpcCallExecutor(), new ZfooRpcPendingResponses());
    }

    public ZfooRpcClientAdapter(Session session, Object attachment, Sender sender,
                                RpcCallExecutor executor, ZfooRpcPendingResponses pending) {
        this.session = Objects.requireNonNull(session, "session");
        this.attachment = attachment;
        this.sender = Objects.requireNonNull(sender, "sender");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.pending = Objects.requireNonNull(pending, "pending");
    }

    public <T> CompletionStage<T> call(String destination, String commandId, Object payload,
                                       long deadlineMillis, Class<T> responseType,
                                       RpcCallExecutor.RetryMode retryMode,
                                       Predicate<Throwable> retryable) {
        Objects.requireNonNull(responseType, "responseType");
        Objects.requireNonNull(retryMode, "retryMode");
        Objects.requireNonNull(retryable, "retryable");
        var request = new GameRpcClient.Request<>(destination, commandId, payload, deadlineMillis);
        long now = System.currentTimeMillis();
        CompletionStage<byte[]> wireResponse = pending.register(commandId, deadlineMillis, now);
        java.util.function.Function<byte[], T> decoder = bytes -> ZfooRpcCodec.decode(bytes, responseType);
        decoders.put(commandId, decoder);
        CompletionStage<T> result = executor.execute(destination, commandId, retryMode, deadlineMillis,
                () -> sender.send(session, ZfooRpcCodec.request(request), attachment).thenCompose(ignored -> wireResponse),
                retryable).thenApply(decoder);
        result.whenComplete((ignored, error) -> {
            decoders.remove(commandId);
            if (error != null) pending.fail(commandId, error, System.currentTimeMillis());
        });
        return result;
    }

    public RpcPendingCalls.ReplyResult accept(RpcResponseEnvelope response, long nowMillis) {
        Objects.requireNonNull(response, "response");
        return pending.accept(response, nowMillis);
    }

    public Object decode(RpcResponseEnvelope response) {
        Objects.requireNonNull(response, "response");
        var decoder = decoders.get(response.getCommandId());
        if (decoder == null) throw new IllegalArgumentException("unknown commandId: " + response.getCommandId());
        return decoder.apply(response.getPayload());
    }

    public int expire(long nowMillis) { return pending.expire(nowMillis); }
    public RpcCallExecutor.Stats stats() { return executor.stats(); }

    @Override
    public void close() {
        decoders.clear();
        pending.close();
        executor.close();
    }
}