package io.gameframe.transport;

import com.zfoo.net.session.Session;
import io.gameframe.runtime.RpcIdempotencyRegistry;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Decodes migration commands and returns typed, idempotent replies through ZfooRpcServerHandler. */
public final class ZfooPlayerMigrationHandler implements ZfooRpcServerHandler.Handler, AutoCloseable {
    @FunctionalInterface
    public interface Operation {
        CompletionStage<Boolean> apply(PlayerMigrationCommand command);
    }

    private final Operation operation;
    private final RpcIdempotencyRegistry<byte[]> idempotency;

    public ZfooPlayerMigrationHandler(Operation operation) {
        this(operation, new RpcIdempotencyRegistry<>());
    }

    public ZfooPlayerMigrationHandler(Operation operation, RpcIdempotencyRegistry<byte[]> idempotency) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.idempotency = Objects.requireNonNull(idempotency, "idempotency");
    }

    @Override
    public CompletionStage<byte[]> handle(Session session, RpcRequestEnvelope request, Object attachment) {
        Objects.requireNonNull(request, "request");
        return idempotency.execute(request.getCommandId(), System.currentTimeMillis(), () -> {
            PlayerMigrationCommand command = ZfooRpcCodec.decode(request.getPayload(), PlayerMigrationCommand.class);
            return operation.apply(command).thenApply(accepted ->
                    ZfooRpcCodec.encode(new PlayerMigrationReply(Boolean.TRUE.equals(accepted),
                            Boolean.TRUE.equals(accepted) ? null : "migration operation rejected")));
        }).stage();
    }

    public int retainedCommands() { return idempotency.size(); }
    public int purge(long nowMillis) { return idempotency.purge(nowMillis); }

    @Override public void close() { idempotency.close(); }
}