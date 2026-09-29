package io.gameframe.transport;

import io.gameframe.runtime.RpcCallExecutor;
import io.gameframe.scene.CrossProcessPlayerMigration;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/**
 * zfoo RPC implementation of a player migration node. Every phase uses a
 * stable command id, so a retry or a duplicate delivery can be handled by the
 * remote node as an idempotent operation.
 */
public final class ZfooPlayerMigrationNode implements CrossProcessPlayerMigration.Node {
    private final String destination;
    private final ZfooRpcClientAdapter rpc;
    private final Predicate<Throwable> retryable;

    public ZfooPlayerMigrationNode(String destination, ZfooRpcClientAdapter rpc) {
        this(destination, rpc, error -> true);
    }

    public ZfooPlayerMigrationNode(String destination, ZfooRpcClientAdapter rpc,
                                   Predicate<Throwable> retryable) {
        if (destination == null || destination.isBlank()) throw new IllegalArgumentException("destination required");
        this.destination = destination;
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.retryable = Objects.requireNonNull(retryable, "retryable");
    }

    @Override public CompletionStage<Boolean> freeze(CrossProcessPlayerMigration.Request request) {
        return booleanCall(request, PlayerMigrationCommand.FREEZE);
    }
    @Override public CompletionStage<Void> release(CrossProcessPlayerMigration.Request request) {
        return voidCall(request, PlayerMigrationCommand.RELEASE);
    }
    @Override public CompletionStage<Void> resume(CrossProcessPlayerMigration.Request request) {
        return voidCall(request, PlayerMigrationCommand.RESUME);
    }
    @Override public CompletionStage<Boolean> prepare(CrossProcessPlayerMigration.Request request) {
        return booleanCall(request, PlayerMigrationCommand.PREPARE);
    }
    @Override public CompletionStage<Void> commit(CrossProcessPlayerMigration.Request request) {
        return voidCall(request, PlayerMigrationCommand.COMMIT);
    }
    @Override public CompletionStage<Void> cancel(CrossProcessPlayerMigration.Request request) {
        return voidCall(request, PlayerMigrationCommand.CANCEL);
    }

    private CompletionStage<Boolean> booleanCall(CrossProcessPlayerMigration.Request request, int operation) {
        return call(request, operation).thenApply(PlayerMigrationReply::isAccepted);
    }

    private CompletionStage<Void> voidCall(CrossProcessPlayerMigration.Request request, int operation) {
        return call(request, operation).thenCompose(reply -> {
            if (reply.isAccepted()) return CompletableFuture.completedFuture(null);
            return CompletableFuture.failedFuture(new IllegalStateException(
                    reply.getMessage() == null ? "migration operation rejected" : reply.getMessage()));
        });
    }

    private CompletionStage<PlayerMigrationReply> call(CrossProcessPlayerMigration.Request request, int operation) {
        Objects.requireNonNull(request, "request");
        var command = new PlayerMigrationCommand(operation, request.playerId(), request.sourceInstance(),
                request.targetInstance(), request.fencingToken(), request.deadlineMillis(), request.snapshot());
        String commandId = "player-migration:" + request.playerId() + ":" + request.fencingToken() + ":" + operation;
        return rpc.call(destination, commandId, command, request.deadlineMillis(), PlayerMigrationReply.class,
                RpcCallExecutor.RetryMode.IDEMPOTENT, retryable);
    }
}