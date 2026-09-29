package io.gameframe.demo;

import com.zfoo.net.NetContext;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.router.IRouter;
import com.zfoo.net.session.Session;
import com.zfoo.protocol.ProtocolManager;
import io.gameframe.runtime.CrossProcessMigrationCoordinator;
import io.gameframe.scene.CrossProcessPlayerMigration;
import io.gameframe.transport.GameTcpServer;
import io.gameframe.transport.InboundTrafficGuard;
import io.gameframe.transport.PlayerMigrationCommand;
import io.gameframe.transport.RpcResponseEnvelope;
import io.gameframe.transport.ZfooPlayerMigrationHandler;
import io.gameframe.transport.ZfooPlayerMigrationNode;
import io.gameframe.transport.ZfooRpcClientAdapter;
import io.gameframe.transport.ZfooSender;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericXmlApplicationContext;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises player migration across two independently listening zfoo TCP nodes. */
class PlayerMigrationTcpExerciseTest {
    @Test
    void migratesPlayerAcrossTwoRealTcpNodesBeforePublishingRoute() throws Exception {
        GenericXmlApplicationContext context = null;
        if (!ProtocolManager.isProtocolClass(PlayerMigrationCommand.class)) {
            context = new GenericXmlApplicationContext();
            context.load("game-net.xml");
            context.refresh();
        }
        var events = new CopyOnWriteArrayList<String>();
        var sourceServer = server(NetContext.getRouter(), command -> {
            events.add("source:" + operation(command.getOperation()));
            return CompletableFuture.completedFuture(true);
        });
        var targetServer = server(NetContext.getRouter(), command -> {
            events.add("target:" + operation(command.getOperation()));
            return CompletableFuture.completedFuture(true);
        });
        try {
            sourceServer.start();
            targetServer.start();
            try (var sourceClient = new SocketRpcClient(sourceServer.boundPort());
                 var targetClient = new SocketRpcClient(targetServer.boundPort());
                 var coordinator = new CrossProcessMigrationCoordinator(8, 64 * 1024)) {
                var migration = new CrossProcessPlayerMigration(coordinator,
                        (playerId, targetInstance, fencingToken) -> {
                            events.add("route:" + targetInstance + ":" + fencingToken);
                            return CompletableFuture.completedFuture(null);
                        });
                long deadline = System.currentTimeMillis() + 5_000;
                var request = new CrossProcessPlayerMigration.Request("player-42", "game-a", "game-b", 7,
                        new byte[] {4, 2}, deadline);
                var result = migration.begin(request,
                        new ZfooPlayerMigrationNode("game-a", sourceClient.adapter(), error -> false),
                        new ZfooPlayerMigrationNode("game-b", targetClient.adapter(), error -> false))
                        .result().toCompletableFuture().get(5, TimeUnit.SECONDS);

                assertEquals(CrossProcessMigrationCoordinator.Outcome.ACCEPTED, result.outcome());
                assertEquals(List.of("source:freeze", "target:prepare", "source:release",
                        "target:commit", "route:game-b:7"), events);
                assertEquals(0, coordinator.pendingCount());
            }
        } finally {
            targetServer.shutdown();
            sourceServer.shutdown();
            if (context != null) context.close();
        }
    }

    private static NodeServer server(IRouter router, ZfooPlayerMigrationHandler.Operation operation) {
        var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                128, 1024 * 1024, Duration.ofSeconds(1), Duration.ofSeconds(30)));
        var rpc = new io.gameframe.transport.ZfooRpcServerHandler(new ZfooSender(1024 * 1024, 256 * 1024),
                new ZfooPlayerMigrationHandler(operation));
        return new NodeServer(guard, router, rpc);
    }

    private static String operation(int value) {
        return switch (value) {
            case PlayerMigrationCommand.FREEZE -> "freeze";
            case PlayerMigrationCommand.RELEASE -> "release";
            case PlayerMigrationCommand.RESUME -> "resume";
            case PlayerMigrationCommand.PREPARE -> "prepare";
            case PlayerMigrationCommand.COMMIT -> "commit";
            case PlayerMigrationCommand.CANCEL -> "cancel";
            default -> throw new IllegalArgumentException("unknown operation: " + value);
        };
    }

    private static final class NodeServer extends GameTcpServer {
        private NodeServer(InboundTrafficGuard guard, IRouter router,
                           io.gameframe.transport.ZfooRpcServerHandler rpc) {
            super(HostAndPort.valueOf("127.0.0.1", 0), guard, router, null, rpc);
        }
        private int boundPort() {
            return ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }
    }

    private static final class SocketRpcClient implements AutoCloseable {
        private final Socket socket;
        private final DataInputStream input;
        private final EmbeddedChannel localChannel = new EmbeddedChannel();
        private final ZfooRpcClientAdapter adapter;
        private final Thread reader;

        private SocketRpcClient(int port) throws Exception {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout(6_000);
            input = new DataInputStream(socket.getInputStream());
            adapter = new ZfooRpcClientAdapter(new Session(localChannel), null,
                    (session, request, attachment) -> {
                        var encoded = Unpooled.buffer();
                        try {
                            NetContext.getPacketService().writeHeaderAndBody(encoded, request, null);
                            byte[] bytes = new byte[encoded.readableBytes()];
                            encoded.readBytes(bytes);
                            synchronized (socket) {
                                socket.getOutputStream().write(bytes);
                                socket.getOutputStream().flush();
                            }
                            return CompletableFuture.completedFuture(null);
                        } catch (Throwable error) {
                            return CompletableFuture.failedFuture(error);
                        } finally {
                            encoded.release();
                        }
                    });
            reader = Thread.ofVirtual().name("migration-rpc-response").start(this::readResponses);
        }

        private ZfooRpcClientAdapter adapter() { return adapter; }

        private void readResponses() {
            try {
                while (!socket.isClosed()) {
                    int length = input.readInt();
                    if (length <= 0 || length > 1024 * 1024) throw new IllegalStateException("invalid RPC frame length");
                    byte[] bytes = input.readNBytes(length);
                    if (bytes.length != length) throw new IllegalStateException("truncated RPC frame");
                    var buffer = Unpooled.wrappedBuffer(bytes);
                    try {
                        Object packet = NetContext.getPacketService().read(buffer).getPacket();
                        if (!(packet instanceof RpcResponseEnvelope response))
                            throw new IllegalStateException("unexpected RPC response: " + packet.getClass().getName());
                        adapter.accept(response, System.currentTimeMillis());
                    } finally {
                        buffer.release();
                    }
                }
            } catch (SocketException ignored) {
                // Normal close wakes the reader.
            } catch (Throwable error) {
                if (!socket.isClosed()) throw new RuntimeException(error);
            }
        }

        @Override public void close() throws Exception {
            adapter.close();
            socket.close();
            reader.join(1_000);
            localChannel.finishAndReleaseAll();
        }
    }
}