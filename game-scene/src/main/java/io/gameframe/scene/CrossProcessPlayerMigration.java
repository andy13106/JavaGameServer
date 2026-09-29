package io.gameframe.scene;

import io.gameframe.runtime.CrossProcessMigrationCoordinator;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Business-facing bridge from a player/actor snapshot to the cross-process
 * migration state machine. Node implementations are normally RPC clients;
 * route publication happens only after the target commit has completed.
 */
public final class CrossProcessPlayerMigration {
    public record Request(String playerId, String sourceInstance, String targetInstance,
                          long fencingToken, byte[] snapshot, long deadlineMillis) {
        public Request {
            requireText(playerId, "playerId");
            requireText(sourceInstance, "sourceInstance");
            requireText(targetInstance, "targetInstance");
            if (sourceInstance.equals(targetInstance) || fencingToken < 1 || deadlineMillis < 1)
                throw new IllegalArgumentException("invalid player migration request");
            snapshot = Objects.requireNonNull(snapshot, "snapshot").clone();
        }
        @Override public byte[] snapshot() { return snapshot.clone(); }
    }

    /** A remote Game node must make each operation idempotent by migration/player id. */
    public interface Node {
        CompletionStage<Boolean> freeze(Request request);
        CompletionStage<Void> release(Request request);
        CompletionStage<Void> resume(Request request);
        CompletionStage<Boolean> prepare(Request request);
        CompletionStage<Void> commit(Request request);
        CompletionStage<Void> cancel(Request request);
    }

    /** Publishes the new owner only after target commit and fencing succeed. */
    public interface RouteOwner {
        CompletionStage<Void> publish(String playerId, String targetInstance, long fencingToken);
    }

    private final CrossProcessMigrationCoordinator coordinator;
    private final RouteOwner routes;

    public CrossProcessPlayerMigration(CrossProcessMigrationCoordinator coordinator, RouteOwner routes) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.routes = Objects.requireNonNull(routes, "routes");
    }

    public CrossProcessMigrationCoordinator.Migration begin(Request request, Node source, Node target) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        var runtimeRequest = new CrossProcessMigrationCoordinator.Request(
                "player-migration:" + request.playerId(), request.playerId(), request.sourceInstance(),
                request.targetInstance(), request.fencingToken(), request.snapshot(), request.deadlineMillis());
        return coordinator.begin(runtimeRequest,
                new CrossProcessMigrationCoordinator.Source() {
                    public CompletionStage<Boolean> freeze(CrossProcessMigrationCoordinator.Request ignored) {
                        return source.freeze(request);
                    }
                    public CompletionStage<Void> release(CrossProcessMigrationCoordinator.Request ignored) {
                        return source.release(request);
                    }
                    public CompletionStage<Void> resume(CrossProcessMigrationCoordinator.Request ignored) {
                        return source.resume(request);
                    }
                },
                new CrossProcessMigrationCoordinator.Target() {
                    public CompletionStage<Boolean> prepare(CrossProcessMigrationCoordinator.Request ignored) {
                        return target.prepare(request);
                    }
                    public CompletionStage<Void> commit(CrossProcessMigrationCoordinator.Request ignored) {
                        return target.commit(request).thenCompose(done ->
                                routes.publish(request.playerId(), request.targetInstance(), request.fencingToken()));
                    }
                    public CompletionStage<Void> cancel(CrossProcessMigrationCoordinator.Request ignored) {
                        return target.cancel(request);
                    }
                });
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }
}