package io.gameframe.transport;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RpcConnectionPoolTest {
    @Test
    void evictsFailedConnectionAndReconnectsOnNextCall() {
        var opened = new AtomicInteger();
        try (var pool = new RpcConnectionPool(destination -> {
            int index = opened.getAndIncrement();
            return new TestConnection(request -> index == 0
                    ? CompletableFuture.failedFuture(new IllegalStateException("broken"))
                    : CompletableFuture.completedFuture(request.payload()));
        }, new RpcConnectionPool.Config(1, 2))) {
            var first = request("game-a", "one", "payload-1");
            assertThrows(Exception.class, () -> pool.send(first).toCompletableFuture().join());
            assertEquals(0, pool.openConnectionCount("game-a"));
            assertEquals("payload-2", pool.send(request("game-a", "two", "payload-2"))
                    .toCompletableFuture().join());
            assertEquals(2, opened.get());
        }
    }

    @Test
    void enforcesPerConnectionInflightLimit() {
        var pending = new CompletableFuture<String>();
        try (var pool = new RpcConnectionPool(destination -> new TestConnection(ignored -> pending),
                new RpcConnectionPool.Config(1, 1))) {
            var first = pool.send(request("game-b", "one", "first"));
            var second = pool.send(request("game-b", "two", "second"));

            var rejected = assertThrows(java.util.concurrent.CompletionException.class, () -> second.toCompletableFuture().join());
            assertInstanceOf(RejectedExecutionException.class, rejected.getCause());
            pending.complete("done");
            assertEquals("done", first.toCompletableFuture().join());
        }
    }

    @Test
    void opensSecondConnectionWhenFirstIsBusy() {
        var opened = new AtomicInteger();
        var pending = new ArrayList<CompletableFuture<String>>();
        try (var pool = new RpcConnectionPool(destination -> {
            opened.incrementAndGet();
            var future = new CompletableFuture<String>();
            synchronized (pending) { pending.add(future); }
            return new TestConnection(ignored -> future);
        }, new RpcConnectionPool.Config(2, 1))) {
            var first = pool.send(request("game-c", "one", "first"));
            var second = pool.send(request("game-c", "two", "second"));
            assertEquals(2, opened.get());
            assertEquals(2, pool.openConnectionCount("game-c"));
            synchronized (pending) {
                pending.get(0).complete("one");
                pending.get(1).complete("two");
            }
            assertEquals("one", first.toCompletableFuture().join());
            assertEquals("two", second.toCompletableFuture().join());
        }
    }

    private static GameRpcClient.Request<String> request(String destination, String command, String payload) {
        return new GameRpcClient.Request<>(destination, command, payload, System.currentTimeMillis() + 5_000);
    }

    @FunctionalInterface
    private interface Sender { CompletionStage<?> send(GameRpcClient.Request<?> request); }

    private static final class TestConnection implements RpcConnectionPool.Connection {
        private final Sender sender;
        private final AtomicBoolean open = new AtomicBoolean(true);
        private TestConnection(Sender sender) { this.sender = sender; }
        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletionStage<T> send(GameRpcClient.Request<T> request) {
            return (CompletionStage<T>) sender.send(request);
        }
        @Override public boolean isOpen() { return open.get(); }
        @Override public void close() { open.set(false); }
    }
}