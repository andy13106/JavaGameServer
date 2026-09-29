package io.gameframe.demo;

import com.zfoo.net.NetContext;
import com.zfoo.net.session.Session;
import com.zfoo.protocol.ProtocolManager;
import io.gameframe.runtime.CrossProcessMigrationCoordinator;
import io.gameframe.scene.CrossProcessPlayerMigration;
import io.gameframe.transport.PlayerMigrationCommand;
import io.gameframe.transport.RpcResponseEnvelope;
import io.gameframe.transport.ZfooPlayerMigrationNode;
import io.gameframe.transport.ZfooRpcClientAdapter;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Connects to two externally started GameFrame containers and exercises migration over TCP. */
class PlayerMigrationContainerExerciseIT {
    @Test
    void migratesPlayerAcrossContainers() throws Exception {
        String sourcePort = System.getenv("GAME_MIGRATION_SOURCE_PORT");
        String targetPort = System.getenv("GAME_MIGRATION_TARGET_PORT");
        Assumptions.assumeTrue(sourcePort != null && targetPort != null,
                "set GAME_MIGRATION_SOURCE_PORT and GAME_MIGRATION_TARGET_PORT to run container exercise");
        if (!ProtocolManager.isProtocolClass(PlayerMigrationCommand.class)) {
            try (var context = new org.springframework.context.support.GenericXmlApplicationContext()) {
                context.load("game-net.xml");
                context.refresh();
            }
        }
        try (var sourceClient = new SocketRpcClient(Integer.parseInt(sourcePort));
             var targetClient = new SocketRpcClient(Integer.parseInt(targetPort));
             var coordinator = new CrossProcessMigrationCoordinator(8, 64 * 1024)) {
            var routes = new CopyOnWriteArrayList<String>();
            var migration = new CrossProcessPlayerMigration(coordinator,
                    (playerId, target, token) -> {
                        routes.add(playerId + "@" + target + ":" + token);
                        return CompletableFuture.completedFuture(null);
                    });
            var request = new CrossProcessPlayerMigration.Request("container-player", "game-a", "game-b", 21,
                    new byte[] {2, 1, 0}, System.currentTimeMillis() + 10_000);
            var result = migration.begin(request,
                    new ZfooPlayerMigrationNode("game-a", sourceClient.adapter(), error -> false),
                    new ZfooPlayerMigrationNode("game-b", targetClient.adapter(), error -> false))
                    .result().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertTrue(result.accepted(), "container migration should be accepted: " + result.error());
            assertEquals(List.of("container-player@game-b:21"), routes);
            assertEquals(0, coordinator.pendingCount());
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
            socket.setSoTimeout(10_000);
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
            reader = Thread.ofVirtual().start(this::readResponses);
        }

        private ZfooRpcClientAdapter adapter() { return adapter; }

        private void readResponses() {
            try {
                while (!socket.isClosed()) {
                    int length = input.readInt();
                    if (length <= 0 || length > 1024 * 1024) throw new IllegalStateException("invalid RPC frame");
                    byte[] bytes = input.readNBytes(length);
                    if (bytes.length != length) throw new IllegalStateException("truncated RPC frame");
                    var buffer = Unpooled.wrappedBuffer(bytes);
                    try {
                        Object packet = NetContext.getPacketService().read(buffer).getPacket();
                        if (!(packet instanceof RpcResponseEnvelope response)) throw new IllegalStateException("unexpected response");
                        adapter.accept(response, System.currentTimeMillis());
                    } finally {
                        buffer.release();
                    }
                }
            } catch (SocketException ignored) {
            } catch (Throwable error) {
                if (!socket.isClosed()) throw new RuntimeException(error);
            }
        }

        @Override public void close() throws Exception {
            adapter.close();
            socket.close();
            reader.join(2_000);
            localChannel.finishAndReleaseAll();
        }
    }
}