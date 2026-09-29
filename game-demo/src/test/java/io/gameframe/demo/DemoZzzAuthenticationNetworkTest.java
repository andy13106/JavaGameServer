package io.gameframe.demo;

import com.zfoo.net.NetContext;
import com.zfoo.net.anno.PacketReceiver;
import com.zfoo.net.anno.Task;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.router.attachment.UdpAttachment;
import com.zfoo.net.session.Session;
import com.zfoo.protocol.ProtocolManager;
import io.gameframe.demo.protocol.AuthenticationRequest;
import io.gameframe.demo.protocol.AuthenticationResponse;
import io.gameframe.transport.ActorAwareZfooRouter;
import io.gameframe.transport.GameAuthenticationService;
import io.gameframe.transport.GameSessionBinder;
import io.gameframe.transport.GameSessionRouter;
import io.gameframe.transport.GameTcpServer;
import io.gameframe.transport.GameUdpServer;
import io.gameframe.transport.GameWebsocketServer;
import io.gameframe.transport.InboundTrafficGuard;
import io.gameframe.transport.ZfooSender;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericXmlApplicationContext;

import java.io.DataInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoZzzAuthenticationNetworkTest {
    @Test
    void realClientsAuthenticateAndRejectReplayAcrossTransports() throws Exception {
        GenericXmlApplicationContext context = null;
        if (!ProtocolManager.isProtocolClass(AuthenticationRequest.class)) {
            context = new GenericXmlApplicationContext();
            context.load("game-net.xml");
            context.refresh();
        }

        try (var sessions = new GameSessionRouter(2, 8, 32, 4)) {
            var authentication = authentication(sessions);
            var router = new ActorAwareZfooRouter(sessions);
            router.registerPacketReceiverDefinition(new AuthenticationReceiver(authentication));
            var traffic = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                    64, 64 * 1024, Duration.ofSeconds(1), Duration.ofSeconds(30)));

            var tcp = new TestTcpServer(traffic, router);
            try {
                tcp.start();
                authenticateTcp(tcp.boundPort());
            } finally {
                tcp.shutdown();
            }

            var websocket = new TestWebsocketServer(traffic, router);
            try {
                websocket.start();
                authenticateWebsocket(websocket.boundPort());
            } finally {
                websocket.shutdown();
            }

            var udp = new TestUdpServer(traffic, router);
            try {
                udp.start();
                authenticateUdp(udp.boundPort());
            } finally {
                udp.shutdown();
            }
        } finally {
            if (context != null) {
                context.close();
            }
        }
    }

    private static GameAuthenticationService authentication(GameSessionRouter sessions) {
        var replay = new io.gameframe.transport.ReplayGuard(
                new io.gameframe.transport.ReplayGuard.Limits(
                        128, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofMinutes(2)));
        return new GameAuthenticationService(new GameSessionBinder(sessions), replay,
                identity -> switch (identity) {
                    case "account-7" -> Optional.of(account(7, "secret-7"));
                    case "account-8" -> Optional.of(account(8, "secret-8"));
                    case "account-9" -> Optional.of(account(9, "secret-9"));
                    default -> Optional.empty();
                });
    }

    private static GameAuthenticationService.Account account(long uid, String secret) {
        return new GameAuthenticationService.Account(uid, secret.getBytes(StandardCharsets.UTF_8));
    }
    private static void authenticateTcp(int port) throws Exception {
        try (var socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000);
            var input = new DataInputStream(socket.getInputStream());
            long timestamp = System.currentTimeMillis();
            var request = request("account-7", timestamp, "nonce-tcp", "wrong");
            write(socket, request);
            var invalid = read(input);
            assertFalse(invalid.isAccepted());
            assertEquals("INVALID_CREDENTIALS", invalid.getRejection());

            request = request("account-7", timestamp, "nonce-tcp", "secret-7");
            write(socket, request);
            var accepted = read(input);
            assertTrue(accepted.isAccepted(), accepted.getRejection());
            assertEquals(7, accepted.getUid());

            write(socket, request);
            var duplicate = read(input);
            assertFalse(duplicate.isAccepted());
            assertEquals("REPLAY", duplicate.getRejection());
        }
    }

    private static void authenticateWebsocket(int port) throws Exception {
        var response = new CompletableFuture<AuthenticationResponse>();
        var request = request("account-8", System.currentTimeMillis(), "nonce-ws", "secret-8");
        var websocket = HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + port + "/websocket"), new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        webSocket.sendBinary(ByteBuffer.wrap(encode(request)), true);
                        WebSocket.Listener.super.onOpen(webSocket);
                    }

                    @Override
                    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                        if (last) {
                            response.complete(decode(data));
                        }
                        return WebSocket.Listener.super.onBinary(webSocket, data, last);
                    }
                }).get(5, TimeUnit.SECONDS);
        try {
            var result = response.get(5, TimeUnit.SECONDS);
            assertTrue(result.isAccepted(), result.getRejection());
            assertEquals(8, result.getUid());
        } finally {
            websocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
        }
    }

    private static void authenticateUdp(int port) throws Exception {
        try (var socket = new DatagramSocket()) {
            socket.setSoTimeout(5000);
            var bytes = encode(request("account-9", System.currentTimeMillis(), "nonce-udp", "secret-9"));
            socket.send(new DatagramPacket(bytes, bytes.length, InetAddress.getLoopbackAddress(), port));
            var packet = new DatagramPacket(new byte[65_507], 65_507);
            socket.receive(packet);
            var result = decode(ByteBuffer.wrap(packet.getData(), packet.getOffset(), packet.getLength()));
            assertTrue(result.isAccepted(), result.getRejection());
            assertEquals(9, result.getUid());
        }
    }
    private static AuthenticationRequest request(String identity, long timestamp, String nonce, String secret) {
        var request = new AuthenticationRequest();
        request.setIdentity(identity);
        request.setTimestampMillis(timestamp);
        request.setNonce(nonce);
        request.setSecret(secret);
        return request;
    }
    private static void write(Socket socket, AuthenticationRequest request) throws Exception {
        byte[] bytes = encode(request);
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
    }

    private static byte[] encode(AuthenticationRequest request) {
        ByteBuf encoded = Unpooled.buffer();
        try {
            NetContext.getPacketService().writeHeaderAndBody(encoded, request, null);
            byte[] bytes = new byte[encoded.readableBytes()];
            encoded.readBytes(bytes);
            return bytes;
        } finally {
            encoded.release();
        }
    }

    private static AuthenticationResponse read(DataInputStream input) throws Exception {
        int length = input.readInt();
        assertTrue(length > 0 && length < 1024 * 1024);
        byte[] bytes = input.readNBytes(length);
        assertEquals(length, bytes.length);
        return decode(ByteBuffer.wrap(bytes));
    }

    private static AuthenticationResponse decode(ByteBuffer bytes) {
        ByteBuf encoded = Unpooled.wrappedBuffer(bytes);
        try {
            if (encoded.readableBytes() >= Integer.BYTES
                    && encoded.getInt(encoded.readerIndex()) == encoded.readableBytes() - Integer.BYTES) {
                encoded.readInt();
            }
            return (AuthenticationResponse) NetContext.getPacketService().read(encoded).getPacket();
        } finally {
            encoded.release();
        }
    }

    public static final class AuthenticationReceiver {
        private final GameAuthenticationService authentication;
        private final ZfooSender sender = new ZfooSender(1024 * 1024, 128 * 1024);

        public AuthenticationReceiver(GameAuthenticationService authentication) {
            this.authentication = authentication;
        }

        @PacketReceiver(Task.NettyIO)
        public void atAuthenticationRequest(Session session, AuthenticationRequest request, UdpAttachment attachment) {
            var credentials = new GameAuthenticationService.Credentials(
                    request.getIdentity(), request.getTimestampMillis(), request.getNonce(),
                    request.getSecret() == null ? new byte[0]
                            : request.getSecret().getBytes(StandardCharsets.UTF_8));
            authentication.authenticate(session, credentials).thenAccept(result -> {
                var response = new AuthenticationResponse();
                response.setAccepted(result.accepted());
                response.setRejection(result.rejection() == null ? null : result.rejection().name());
                response.setUid(result.binding() == null ? 0 : result.binding().session().getUid());
                sender.send(session, response, attachment).exceptionally(error -> {
                    session.getChannel().close();
                    return null;
                });
            });
        }
    }

    private static final class TestTcpServer extends GameTcpServer {
        private TestTcpServer(InboundTrafficGuard traffic, ActorAwareZfooRouter router) {
            super(HostAndPort.valueOf("127.0.0.1", 0), traffic, router);
        }
        private int boundPort() {
            return ((java.net.InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }
    }

    private static final class TestWebsocketServer extends GameWebsocketServer {
        private TestWebsocketServer(InboundTrafficGuard traffic, ActorAwareZfooRouter router) {
            super(HostAndPort.valueOf("127.0.0.1", 0), traffic, router);
        }
        private int boundPort() {
            return ((java.net.InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }
    }

    private static final class TestUdpServer extends GameUdpServer {
        private TestUdpServer(InboundTrafficGuard traffic, ActorAwareZfooRouter router) {
            super(HostAndPort.valueOf("127.0.0.1", 0), traffic, router);
        }
        private int boundPort() {
            return ((java.net.InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }
    }
}