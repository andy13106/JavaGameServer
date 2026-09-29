package io.gameframe.demo;

import com.zfoo.net.NetContext;
import com.zfoo.net.session.Session;
import com.zfoo.protocol.ProtocolManager;
import io.gameframe.runtime.CrossProcessMigrationCoordinator;
import io.gameframe.scene.CrossProcessPlayerMigration;
import io.gameframe.transport.PlayerMigrationCommand;
import io.gameframe.transport.RpcResponseEnvelope;
import io.gameframe.transport.ZfooRpcClientAdapter;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies migration over two independently started JVMs and real TCP sockets. */
class PlayerMigrationCrossJvmExerciseTest {
    @Test
    void migratesPlayerAcrossTwoJvmProcesses() throws Exception {
        GenericProtocolBootstrap.ensureLoaded();
        var source = ChildNode.start("game-a");
        var target = ChildNode.start("game-b");
        try (source; target;
             var sourceClient = new SocketRpcClient(source.port());
             var targetClient = new SocketRpcClient(target.port());
             var coordinator = new CrossProcessMigrationCoordinator(8, 64 * 1024)) {
            var routes = new CopyOnWriteArrayList<String>();
            var migration = new CrossProcessPlayerMigration(coordinator,
                    (playerId, targetInstance, fencingToken) -> {
                        routes.add(playerId + "@" + targetInstance + ":" + fencingToken);
                        return CompletableFuture.completedFuture(null);
                    });
                var request = new CrossProcessPlayerMigration.Request(
                        "cross-jvm-player", "game-a", "game-b", 11,
                        new byte[] {1, 9, 8}, System.currentTimeMillis() + 10_000);
                var result = migration.begin(request,
                        new io.gameframe.transport.ZfooPlayerMigrationNode("game-a", sourceClient.adapter(), error -> false),
                        new io.gameframe.transport.ZfooPlayerMigrationNode("game-b", targetClient.adapter(), error -> false))
                        .result().toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertTrue(result.accepted(), "cross JVM migration should be accepted: " + result.error());
                assertEquals(List.of("cross-jvm-player@game-b:11"), routes);
                assertEquals(0, coordinator.pendingCount());
                assertTrue(source.lines().stream().anyMatch(line -> line.contains("MIGRATION_EVENT game-a freeze")));
                assertTrue(target.lines().stream().anyMatch(line -> line.contains("MIGRATION_EVENT game-b commit")));
        }
    }

    private static final class GenericProtocolBootstrap {
        private static void ensureLoaded() throws Exception {
            if (!ProtocolManager.isProtocolClass(PlayerMigrationCommand.class)) {
                try (var context = new org.springframework.context.support.GenericXmlApplicationContext()) {
                    context.load("game-net.xml");
                    context.refresh();
                }
            }
        }
    }

    private static final class ChildNode implements AutoCloseable {
        private final String name;
        private final Process process;
        private final CompletableFuture<Integer> ready = new CompletableFuture<>();
        private final CopyOnWriteArrayList<String> lines = new CopyOnWriteArrayList<>();
        private final Thread reader;

        private ChildNode(String name, Process process) {
            this.name = name;
            this.process = process;
            this.reader = Thread.ofVirtual().name("migration-child-" + name).start(this::readOutput);
        }

        private static ChildNode start(String name) throws Exception {
            String java = Path.of(System.getProperty("java.home"), "bin",
                    System.getProperty("os.name", "").toLowerCase().contains("win") ? "java.exe" : "java").toString();
            String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            Process process = new ProcessBuilder(java, "-cp", classPath,
                    "io.gameframe.demo.MigrationNodeMain", "--name=" + name, "--port=0")
                    .redirectErrorStream(true).start();
            var child = new ChildNode(name, process);
            child.ready.get(20, TimeUnit.SECONDS);
            return child;
        }

        private void readOutput() {
            try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                    if (line.startsWith("MIGRATION_NODE_READY " + name + " ")) {
                        ready.complete(Integer.parseInt(line.substring(line.lastIndexOf(' ') + 1)));
                    }
                }
                if (!ready.isDone()) {
                    ready.completeExceptionally(new IllegalStateException("child exited before ready: " + lines));
                }
            } catch (Throwable error) {
                if (!ready.isDone()) ready.completeExceptionally(error);
            }
        }

        private int port() throws Exception {
            return ready.get(1, TimeUnit.SECONDS);
        }

        private List<String> lines() {
            return List.copyOf(lines);
        }

        @Override
        public void close() throws Exception {
            try {
                process.getOutputStream().write('\n');
                process.getOutputStream().flush();
            } catch (IOException ignored) {
            }
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
            reader.join(2_000);
            if (process.exitValue() != 0) {
                throw new IllegalStateException("child " + name + " exited with " + process.exitValue() + ": " + lines);
            }
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
            reader = Thread.ofVirtual().name("cross-jvm-rpc-response").start(this::readResponses);
        }

        private ZfooRpcClientAdapter adapter() {
            return adapter;
        }

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
                // Closing the client wakes the response reader.
            } catch (Throwable error) {
                if (!socket.isClosed()) throw new RuntimeException(error);
            }
        }

        @Override
        public void close() throws Exception {
            adapter.close();
            socket.close();
            reader.join(2_000);
            localChannel.finishAndReleaseAll();
        }
    }
}