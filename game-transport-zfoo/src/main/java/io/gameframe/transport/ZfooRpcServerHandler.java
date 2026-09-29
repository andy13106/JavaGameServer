package io.gameframe.transport;

import com.zfoo.net.session.Session;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Dispatches RPC envelopes and sends correlated responses through the existing bounded sender. */
public final class ZfooRpcServerHandler {
    @FunctionalInterface
    public interface Sender {
        CompletionStage<?> send(Session session, RpcResponseEnvelope response, Object attachment);
    }

    @FunctionalInterface
    public interface Handler {
        CompletionStage<byte[]> handle(Session session, RpcRequestEnvelope request, Object attachment);
    }

    private final Sender sender;
    private final Handler handler;

    public ZfooRpcServerHandler(ZfooSender sender, Handler handler) {
        this((session, response, attachment) -> sender.send(session, response, attachment), handler);
    }

    public ZfooRpcServerHandler(Sender sender, Handler handler) {
        this.sender = Objects.requireNonNull(sender, "sender");
        this.handler = Objects.requireNonNull(handler, "handler");
    }

    public void dispatch(Session session, RpcRequestEnvelope request, Object attachment) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(request, "request");
        if (request.getCommandId() == null || request.getCommandId().isBlank()) {
            send(session, request.getCommandId(), false, "commandId required", null, attachment);
            return;
        }
        long now = System.currentTimeMillis();
        if (request.getDeadlineMillis() <= now) {
            send(session, request.getCommandId(), false, "RPC deadline exceeded", null, attachment);
            return;
        }
        CompletionStage<byte[]> result;
        try {
            result = Objects.requireNonNull(handler.handle(session, request, attachment), "handler result");
        } catch (Throwable error) {
            send(session, request.getCommandId(), false, message(error), null, attachment);
            return;
        }
        result.whenComplete((payload, error) -> send(session, request.getCommandId(), error == null,
                error == null ? null : message(error), error == null ? payload : null, attachment));
    }

    private void send(Session session, String commandId, boolean success, String error, byte[] payload, Object attachment) {
        var response = new RpcResponseEnvelope(commandId, success, error, payload);
        try {
            sender.send(session, response, attachment);
        } catch (Throwable ignored) {
            // The bounded sender owns transport failure accounting; the caller cannot receive a second response.
        }
    }

    private static String message(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message == null || message.isBlank() ? cause.getClass().getSimpleName() : message;
    }
}