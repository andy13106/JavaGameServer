package io.gameframe.demo;

import io.gameframe.runtime.ActorSystem;
import io.gameframe.storage.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * One demonstration player. Serial durable commands; bounded waiting requests.
 * Real games can coalesce non-critical dirty fields above DocumentStore.
 */
public final class PlayerService {
    private final ActorSystem.ActorRef actor;
    private final DocumentStore store;
    private final StoreKey key;
    private final ArrayDeque<Request> requests = new ArrayDeque<>();
    private boolean busy;
    private final java.util.concurrent.atomic.AtomicReference<Throwable> terminal = new java.util.concurrent.atomic.AtomicReference<>();
    private final Set<CompletableFuture<WriteResult>> outstanding = ConcurrentHashMap.newKeySet();
    private long version;
    private record Request(Function<Long, WriteCommand> factory, CompletableFuture<WriteResult> reply) {}
    public PlayerService(ActorSystem system, DocumentStore store, String playerId) {
        actor = system.spawn("player:" + playerId); this.store = store; key = new StoreKey("player_core", playerId);
    }
    public CompletionStage<Snapshot> open() {
        CompletableFuture<Snapshot> reply = new CompletableFuture<>();
        actor.tell(() -> actor.pipe(() -> store.load(key), (snapshot, error) -> {
            if (error != null) { reply.completeExceptionally(error); return; }
            if (snapshot.isPresent()) {
                version = snapshot.get().version(); reply.complete(snapshot.get()); return;
            }
            actor.pipe(() -> store.write(key, WriteCommand.create("initial-" + UUID.randomUUID(), Map.of("gold", 100L, "level", 1L))),
                (result, createError) -> {
                    if (createError != null) { reply.completeExceptionally(createError); return; }
                    actor.pipe(() -> store.load(key), (loaded, loadError) -> {
                        if (loadError != null) reply.completeExceptionally(loadError);
                        else if (loaded.isEmpty()) reply.completeExceptionally(new IllegalStateException("player creation failed"));
                        else { version = loaded.get().version(); reply.complete(loaded.get()); }
                    }).exceptionally(e -> { reply.completeExceptionally(e); return null; });
                }).exceptionally(e -> { reply.completeExceptionally(e); return null; });
        }).exceptionally(e -> { reply.completeExceptionally(e); return null; }))
        .exceptionally(e -> { reply.completeExceptionally(e); return null; });
        return reply.minimalCompletionStage();
    }
    public CompletionStage<WriteResult> patch(String operationId, DocumentPatch patch) {
        return enqueue(v -> WriteCommand.patch(v, operationId, patch));
    }
    public CompletionStage<WriteResult> replace(String operationId, Snapshot snapshot) {
        var immutable = snapshot.data();
        return enqueue(v -> WriteCommand.replace(snapshot.version(), operationId, immutable));
    }
    private CompletionStage<WriteResult> enqueue(Function<Long, WriteCommand> factory) {
        CompletableFuture<WriteResult> reply = new CompletableFuture<>();
        outstanding.add(reply); reply.whenComplete((v, e) -> outstanding.remove(reply));
        actor.tell(() -> {
            if (terminal.get() != null || version == 0 || requests.size() >= 64) {
                reply.completeExceptionally(new RejectedExecutionException("player not ready or command queue full")); return;
            }
            requests.add(new Request(factory, reply)); pump();
        }).exceptionally(e -> { reply.completeExceptionally(e); return null; });
        return reply.minimalCompletionStage();
    }
    private void pump() {
        actor.requireCurrent();
        if (busy || requests.isEmpty()) return;
        Request request = requests.remove(); busy = true;
        WriteCommand command;
        try { command = request.factory.apply(version); }
        catch (Throwable error) { busy = false; request.reply.completeExceptionally(error); pump(); return; }
        actor.pipe(() -> store.write(key, command), (result, error) -> {
            busy = false;
            if (error != null || !result.successful()) {
                terminal.compareAndSet(null, error != null ? error : new IllegalStateException("write conflict"));
                Throwable failure = error != null ? error : new IllegalStateException("write conflict: " + result);
                request.reply.completeExceptionally(failure);
                while (!requests.isEmpty()) requests.remove().reply.completeExceptionally(failure);
                return; // do not blindly retry uncertain writes or overwrite another owner
            }
            version = result.version(); request.reply.complete(result); pump();
        }).exceptionally(error -> {
            // Delivery failure may run outside actor. Only complete thread-safe futures here.
            terminal.compareAndSet(null, error);
            outstanding.forEach(f -> f.completeExceptionally(error)); return null;
        });
    }
    public CompletionStage<Optional<Snapshot>> read() { return store.load(key); }
}
