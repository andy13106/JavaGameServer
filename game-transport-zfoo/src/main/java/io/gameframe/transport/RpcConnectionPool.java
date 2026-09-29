package io.gameframe.transport;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded per-destination RPC connection pool.
 *
 * <p>The pool owns connection lifecycle but leaves retries and deadlines to
 * {@link GameRpcClient} or {@code RpcCallExecutor}. A failed connection is
 * evicted after the in-flight operation completes, so the next call opens a
 * fresh connection through the factory.</p>
 */
public final class RpcConnectionPool implements GameRpcClient.Transport, AutoCloseable {
    @FunctionalInterface
    public interface ConnectionFactory {
        Connection open(String destination) throws Exception;
    }

    public interface Connection extends AutoCloseable {
        <T> CompletionStage<T> send(GameRpcClient.Request<T> request);
        boolean isOpen();
        @Override void close();
    }

    public record Config(int maxConnectionsPerDestination, int maxInflightPerConnection) {
        public Config {
            if (maxConnectionsPerDestination < 1) throw new IllegalArgumentException("maxConnectionsPerDestination must be positive");
            if (maxInflightPerConnection < 1) throw new IllegalArgumentException("maxInflightPerConnection must be positive");
        }
        public static Config defaults() { return new Config(2, 128); }
    }

    private final ConnectionFactory factory;
    private final Config config;
    private final ConcurrentHashMap<String, DestinationPool> destinations = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RpcConnectionPool(ConnectionFactory factory) { this(factory, Config.defaults()); }

    public RpcConnectionPool(ConnectionFactory factory, Config config) {
        this.factory = Objects.requireNonNull(factory, "factory");
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public <T> CompletionStage<T> send(GameRpcClient.Request<T> request) {
        Objects.requireNonNull(request, "request");
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("RPC connection pool closed"));
        return destinations.computeIfAbsent(request.destination(), ignored -> new DestinationPool(request.destination())).send(request);
    }

    /** Number of currently open connections for a destination. */
    public int openConnectionCount(String destination) {
        Objects.requireNonNull(destination, "destination");
        var pool = destinations.get(destination);
        return pool == null ? 0 : pool.openCount();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (var pool : destinations.values()) pool.close();
        destinations.clear();
    }

    private final class DestinationPool {
        private final String destination;
        private final ArrayList<Slot> slots = new ArrayList<>();

        private DestinationPool(String destination) { this.destination = destination; }

        private <T> CompletionStage<T> send(GameRpcClient.Request<T> request) {
            Slot slot;
            synchronized (this) {
                if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("RPC connection pool closed"));
                removeClosed();
                slot = selectAvailable();
                if (slot == null && slots.size() < config.maxConnectionsPerDestination()) {
                    try {
                        var connection = Objects.requireNonNull(factory.open(destination), "connection factory returned null");
                        slot = new Slot(connection);
                        slots.add(slot);
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                }
                if (slot == null) {
                    return CompletableFuture.failedFuture(new RejectedExecutionException(
                            "RPC connection pool capacity exhausted for " + destination));
                }
                slot.inflight.incrementAndGet();
            }

            CompletionStage<T> result;
            try {
                result = Objects.requireNonNull(slot.connection.send(request), "connection returned null stage");
            } catch (Throwable error) {
                complete(slot, error);
                return CompletableFuture.failedFuture(error);
            }
            Slot selectedSlot = slot;
            result.whenComplete((ignored, error) -> complete(selectedSlot, error));
            return result;
        }

        private synchronized int openCount() {
            removeClosed();
            return slots.size();
        }

        private synchronized void close() {
            for (var slot : slots) closeQuietly(slot.connection);
            slots.clear();
        }

        private Slot selectAvailable() {
            Slot selected = null;
            for (var candidate : slots) {
                if (candidate.inflight.get() >= config.maxInflightPerConnection()) continue;
                if (selected == null || candidate.inflight.get() < selected.inflight.get()) selected = candidate;
            }
            return selected;
        }

        private void complete(Slot slot, Throwable error) {
            slot.inflight.decrementAndGet();
            if (error != null || !slot.connection.isOpen()) {
                synchronized (this) {
                    if (slots.remove(slot)) closeQuietly(slot.connection);
                }
            }
        }

        private void removeClosed() {
            for (int i = slots.size() - 1; i >= 0; i--) {
                var slot = slots.get(i);
                if (!slot.connection.isOpen() && slot.inflight.get() == 0) {
                    slots.remove(i);
                    closeQuietly(slot.connection);
                }
            }
        }
    }

    private static final class Slot {
        private final Connection connection;
        private final AtomicInteger inflight = new AtomicInteger();
        private Slot(Connection connection) { this.connection = connection; }
    }

    private static void closeQuietly(Connection connection) {
        try { connection.close(); } catch (RuntimeException ignored) { }
    }
}