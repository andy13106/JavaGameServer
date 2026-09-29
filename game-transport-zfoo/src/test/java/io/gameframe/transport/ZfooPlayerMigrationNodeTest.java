package io.gameframe.transport;

import com.zfoo.net.session.Session;
import com.zfoo.protocol.ProtocolManager;
import io.gameframe.scene.CrossProcessPlayerMigration;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ZfooPlayerMigrationNodeTest {
    @BeforeAll
    static void initProtocol() {
        if (!ProtocolManager.isProtocolClass(PlayerMigrationCommand.class)) {
            ProtocolManager.initProtocol(Set.of(RpcRequestEnvelope.class, RpcResponseEnvelope.class,
                    PlayerMigrationCommand.class, PlayerMigrationReply.class));
        }
    }

    @Test
    void nodeUsesStableIdempotentCommandsForAllPhases() throws Exception {
        var channel = new EmbeddedChannel();
        var sent = new ArrayList<RpcRequestEnvelope>();
        try (var adapter = new ZfooRpcClientAdapter(new Session(channel), null,
                (session, request, attachment) -> { sent.add(request); return CompletableFuture.completedFuture(null); })) {
            var node = new ZfooPlayerMigrationNode("game-b", adapter, error -> false);
            var request = new CrossProcessPlayerMigration.Request("p-7", "game-a", "game-b", 4,
                    new byte[] {1, 2, 3}, System.currentTimeMillis() + 5_000);

            var freeze = node.freeze(request).toCompletableFuture();
            assertEquals(1, sent.size());
            var freezeCommand = ZfooRpcCodec.decode(sent.get(0).getPayload(), PlayerMigrationCommand.class);
            assertEquals(PlayerMigrationCommand.FREEZE, freezeCommand.getOperation());
            assertEquals("player-migration:p-7:4:1", sent.get(0).getCommandId());
            assertEquals(io.gameframe.runtime.RpcPendingCalls.ReplyResult.COMPLETED,
                    adapter.accept(new RpcResponseEnvelope(sent.get(0).getCommandId(), true, null,
                            ZfooRpcCodec.encode(new PlayerMigrationReply(true, null))), System.currentTimeMillis()));
            assertTrue(freeze.get(1, TimeUnit.SECONDS));

            var release = node.release(request).toCompletableFuture();
            var releaseCommand = ZfooRpcCodec.decode(sent.get(1).getPayload(), PlayerMigrationCommand.class);
            assertEquals(PlayerMigrationCommand.RELEASE, releaseCommand.getOperation());
            adapter.accept(new RpcResponseEnvelope(sent.get(1).getCommandId(), true, null,
                    ZfooRpcCodec.encode(new PlayerMigrationReply(true, null))), System.currentTimeMillis());
            release.get(1, TimeUnit.SECONDS);

            var prepare = node.prepare(request).toCompletableFuture();
            assertEquals(PlayerMigrationCommand.PREPARE,
                    ZfooRpcCodec.decode(sent.get(2).getPayload(), PlayerMigrationCommand.class).getOperation());
            adapter.accept(new RpcResponseEnvelope(sent.get(2).getCommandId(), true, null,
                    ZfooRpcCodec.encode(new PlayerMigrationReply(true, null))), System.currentTimeMillis());
            assertTrue(prepare.get(1, TimeUnit.SECONDS));

            var commit = node.commit(request).toCompletableFuture();
            assertEquals(PlayerMigrationCommand.COMMIT,
                    ZfooRpcCodec.decode(sent.get(3).getPayload(), PlayerMigrationCommand.class).getOperation());
            adapter.accept(new RpcResponseEnvelope(sent.get(3).getCommandId(), true, null,
                    ZfooRpcCodec.encode(new PlayerMigrationReply(true, null))), System.currentTimeMillis());
            commit.get(1, TimeUnit.SECONDS);

            var cancel = node.cancel(request).toCompletableFuture();
            assertEquals(PlayerMigrationCommand.CANCEL,
                    ZfooRpcCodec.decode(sent.get(4).getPayload(), PlayerMigrationCommand.class).getOperation());
            adapter.accept(new RpcResponseEnvelope(sent.get(4).getCommandId(), true, null,
                    ZfooRpcCodec.encode(new PlayerMigrationReply(true, null))), System.currentTimeMillis());
            cancel.get(1, TimeUnit.SECONDS);

            var resume = node.resume(request).toCompletableFuture();
            assertEquals(PlayerMigrationCommand.RESUME,
                    ZfooRpcCodec.decode(sent.get(5).getPayload(), PlayerMigrationCommand.class).getOperation());
            adapter.accept(new RpcResponseEnvelope(sent.get(5).getCommandId(), true, null,
                    ZfooRpcCodec.encode(new PlayerMigrationReply(true, null))), System.currentTimeMillis());
            resume.get(1, TimeUnit.SECONDS);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void serverHandlerDecodesCommandAndEncodesTypedReply() throws Exception {
        var seen = new ArrayList<PlayerMigrationCommand>();
        var handler = new ZfooPlayerMigrationHandler(command -> {
            seen.add(command);
            return CompletableFuture.completedFuture(command.getOperation() == PlayerMigrationCommand.COMMIT);
        });
        var command = new PlayerMigrationCommand(PlayerMigrationCommand.COMMIT, "p-8", "game-a", "game-b", 5,
                System.currentTimeMillis() + 5_000, new byte[] {9});
        var envelope = new RpcRequestEnvelope("game-b", "migration-command", command.getDeadlineMillis(),
                ZfooRpcCodec.encode(command));
        var payload = handler.handle(new Session(new EmbeddedChannel()), envelope, null)
                .toCompletableFuture().get(1, TimeUnit.SECONDS);
        var reply = ZfooRpcCodec.decode(payload, PlayerMigrationReply.class);
        assertTrue(reply.isAccepted());
        assertEquals(1, seen.size());
        assertEquals(PlayerMigrationCommand.COMMIT, seen.getFirst().getOperation());
    }

    @Test
    void duplicateServerCommandJoinsOriginalOperation() throws Exception {
        var invocations = new AtomicInteger();
        try (var handler = new ZfooPlayerMigrationHandler(command -> {
            invocations.incrementAndGet();
            return CompletableFuture.completedFuture(true);
        })) {
            var command = new PlayerMigrationCommand(PlayerMigrationCommand.COMMIT, "p-9", "game-a", "game-b", 6,
                    System.currentTimeMillis() + 5_000, new byte[] {1});
            var envelope = new RpcRequestEnvelope("game-b", "player-migration:p-9:6:5",
                    command.getDeadlineMillis(), ZfooRpcCodec.encode(command));
            byte[] first = handler.handle(new Session(new EmbeddedChannel()), envelope, null)
                    .toCompletableFuture().get(1, TimeUnit.SECONDS);
            byte[] retry = handler.handle(new Session(new EmbeddedChannel()), envelope, null)
                    .toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertTrue(ZfooRpcCodec.decode(first, PlayerMigrationReply.class).isAccepted());
            assertTrue(ZfooRpcCodec.decode(retry, PlayerMigrationReply.class).isAccepted());
            assertEquals(1, invocations.get());
            assertEquals(1, handler.retainedCommands());
        }
    }}