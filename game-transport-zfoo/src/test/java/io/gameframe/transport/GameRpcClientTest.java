package io.gameframe.transport;

import io.gameframe.runtime.RpcCallExecutor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class GameRpcClientTest {
    @Test
    void retriesThroughTheTransportWithTheSameCommandId() {
        var attempts = new AtomicInteger();
        var requests = new ArrayList<GameRpcClient.Request<?>>();
        var transport = new GameRpcClient.Transport() {
            @Override public <T> java.util.concurrent.CompletionStage<T> send(GameRpcClient.Request<T> request) {
                requests.add(request);
                if (attempts.getAndIncrement() == 0) return CompletableFuture.failedFuture(new IOException("temporary"));
                return CompletableFuture.completedFuture(request.payload());
            }
        };
        var policy = new RpcCallExecutor.Policy(3, Duration.ofMillis(100), Duration.ofMillis(1),
                Duration.ofMillis(5), 5, Duration.ofMillis(20), 8, 32);
        try (var client = new GameRpcClient(transport, new RpcCallExecutor(policy, 1, "rpc-transport-test"))) {
            var request = new GameRpcClient.Request<>("game-1", "command-1", "payload", System.currentTimeMillis() + 1_000);
            assertEquals("payload", client.call(request, RpcCallExecutor.RetryMode.IDEMPOTENT,
                    IOException.class::isInstance).toCompletableFuture().join());
            assertEquals(2, requests.size());
            assertEquals("command-1", requests.get(0).commandId());
            assertEquals(requests.get(0).commandId(), requests.get(1).commandId());
        }
    }

    @Test
    void nonIdempotentTransportCallIsSentOnce() {
        var attempts = new AtomicInteger();
        GameRpcClient.Transport transport = new GameRpcClient.Transport() {
            @Override public <T> java.util.concurrent.CompletionStage<T> send(GameRpcClient.Request<T> request) {
                attempts.incrementAndGet();
                return CompletableFuture.failedFuture(new IOException("temporary"));
            }
        };
        try (var client = new GameRpcClient(transport, new RpcCallExecutor(new RpcCallExecutor.Policy(3, Duration.ofMillis(100), Duration.ZERO,
                Duration.ZERO, 5, Duration.ofMillis(20), 8, 32), 1, "rpc-transport-test"))) {
            var request = new GameRpcClient.Request<>("game-1", "command-1", "payload", System.currentTimeMillis() + 1_000);
            assertThrows(CompletionException.class, () -> client.call(request, RpcCallExecutor.RetryMode.NEVER,
                    IOException.class::isInstance).toCompletableFuture().join());
            assertEquals(1, attempts.get());
        }
    }

    @Test
    void requestRejectsMissingRoutingIdentity() {
        assertThrows(IllegalArgumentException.class,
                () -> new GameRpcClient.Request<>("", "command", "payload", 10));
        assertThrows(IllegalArgumentException.class,
                () -> new GameRpcClient.Request<>("game", "", "payload", 10));
    }
}