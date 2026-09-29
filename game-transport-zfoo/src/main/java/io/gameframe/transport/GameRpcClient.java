package io.gameframe.transport;

import io.gameframe.runtime.RpcCallExecutor;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/** Transport-facing RPC facade shared by TCP, WebSocket and UDP adapters. */
public final class GameRpcClient implements AutoCloseable {
    public record Request<T>(String destination, String commandId, T payload, long deadlineMillis) {
        public Request {
            requireText(destination, "destination");
            requireText(commandId, "commandId");
            if (deadlineMillis <= 0) throw new IllegalArgumentException("deadlineMillis must be positive");
        }
    }

    @FunctionalInterface
    public interface Transport {
        <T> CompletionStage<T> send(Request<T> request);
    }

    private final Transport transport;
    private final RpcCallExecutor executor;

    public GameRpcClient(Transport transport) { this(transport, new RpcCallExecutor()); }
    public GameRpcClient(Transport transport, RpcCallExecutor executor) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public <T> CompletionStage<T> call(Request<T> request,
                                        RpcCallExecutor.RetryMode retryMode,
                                        Predicate<Throwable> retryable) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(retryMode, "retryMode");
        Objects.requireNonNull(retryable, "retryable");
        return executor.execute(request.destination(), request.commandId(), retryMode,
                request.deadlineMillis(), () -> transport.send(request), retryable);
    }

    public RpcCallExecutor.Stats stats() { return executor.stats(); }

    @Override public void close() { executor.close(); }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }
}