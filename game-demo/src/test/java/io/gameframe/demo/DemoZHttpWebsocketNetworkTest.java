package io.gameframe.demo;

import com.zfoo.net.NetContext;
import com.zfoo.net.anno.PacketReceiver;
import com.zfoo.net.anno.Task;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.packet.DecodedPacketInfo;
import com.zfoo.net.router.attachment.HttpAttachment;
import com.zfoo.net.router.IRouter;
import com.zfoo.net.session.Session;
import com.zfoo.protocol.ProtocolManager;
import io.gameframe.demo.protocol.TransportContractRequest;
import io.gameframe.demo.protocol.TransportContractResponse;
import io.gameframe.transport.GameHttpServer;
import io.gameframe.transport.GameWebsocketServer;
import io.gameframe.transport.InboundTrafficGuard;
import io.gameframe.transport.ZfooSender;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoZHttpWebsocketNetworkTest {
    @Test
    void realHttpAndWebsocketClientsRoundTripZfooBinaryProtocol() throws Exception {
        GenericXmlApplicationContext context = null;
        if (ProtocolManager.isProtocolClass(TransportContractRequest.class)) {
            NetContext.getRouter().registerPacketReceiverDefinition(new ContractReceiver());
        } else {
            context = new GenericXmlApplicationContext();
            context.registerBean(ContractReceiver.class, ContractReceiver::new);
            context.load("game-net.xml");
            context.refresh();
        }

        var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                64, 64 * 1024, Duration.ofSeconds(1), Duration.ofSeconds(30)));
        var httpServer = new TestHttpServer(guard, NetContext.getRouter());
        var websocketServer = new TestWebsocketServer(guard, NetContext.getRouter());
        try {
            httpServer.start();
            websocketServer.start();

            byte[] httpRequest = encodeRequest("http-" + UUID.randomUUID());
            var response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + httpServer.boundPort() + "/contract"))
                            .header("Content-Type", "application/octet-stream")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(httpRequest))
                            .build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"success\":true"), response.body());
            assertTrue(response.body().contains("http-"), response.body());

            byte[] websocketRequest = encodeRequest("ws-" + UUID.randomUUID());
            var websocketResponse = new CompletableFuture<byte[]>();
            var received = new java.io.ByteArrayOutputStream();
            var websocket = HttpClient.newHttpClient().newWebSocketBuilder()
                    .buildAsync(URI.create("ws://127.0.0.1:" + websocketServer.boundPort() + "/websocket"),
                            new WebSocket.Listener() {
                                @Override
                                public void onOpen(WebSocket webSocket) {
                                    webSocket.sendBinary(ByteBuffer.wrap(websocketRequest), true);
                                    WebSocket.Listener.super.onOpen(webSocket);
                                }

                                @Override
                                public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                                    var bytes = new byte[data.remaining()];
                                    data.get(bytes);
                                    received.writeBytes(bytes);
                                    if (last) websocketResponse.complete(received.toByteArray());
                                    return WebSocket.Listener.super.onBinary(webSocket, data, last);
                                }
                            }).get(5, TimeUnit.SECONDS);
            var decoded = decodeResponse(websocketResponse.get(5, TimeUnit.SECONDS));
            assertTrue(decoded.isSuccess());
            assertEquals(new String(websocketRequestMarker(websocketRequest)), decoded.getMarker());
            websocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
        } finally {
            websocketServer.shutdown();
            httpServer.shutdown();
            if (context != null) context.close();
        }
    }

    private static byte[] encodeRequest(String marker) {
        var request = new TransportContractRequest();
        request.setMarker(marker);
        ByteBuf buffer = Unpooled.buffer();
        try {
            NetContext.getPacketService().writeHeaderAndBody(buffer, request, null);
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            return bytes;
        } finally {
            buffer.release();
        }
    }

    private static byte[] websocketRequestMarker(byte[] requestBytes) {
        ByteBuf buffer = Unpooled.wrappedBuffer(requestBytes);
        try {
            int length = buffer.readInt();
            var request = (TransportContractRequest) NetContext.getPacketService().read(buffer.readSlice(length)).getPacket();
            return request.getMarker().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        } finally {
            buffer.release();
        }
    }

    private static TransportContractResponse decodeResponse(byte[] bytes) {
        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
        try {
            int length = buffer.readInt();
            return (TransportContractResponse) NetContext.getPacketService().read(buffer.readSlice(length)).getPacket();
        } finally {
            buffer.release();
        }
    }

    private static DecodedPacketInfo resolveHttp(FullHttpRequest request) {
        ByteBuf body = request.content();
        int length = body.readInt();
        var packet = NetContext.getPacketService().read(body.readSlice(length)).getPacket();
        return DecodedPacketInfo.valueOf(packet, HttpAttachment.valueOf(request, HttpResponseStatus.OK));
    }

    private static InboundTrafficGuard guard() {
        return new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                64, 64 * 1024, Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }

    private static final class TestHttpServer extends GameHttpServer {
        private TestHttpServer(InboundTrafficGuard guard, IRouter router) {
            super(HostAndPort.valueOf("127.0.0.1", 0), guard, router, DemoZHttpWebsocketNetworkTest::resolveHttp);
        }
        private int boundPort() { return ((java.net.InetSocketAddress) channelFuture.channel().localAddress()).getPort(); }
    }

    private static final class TestWebsocketServer extends GameWebsocketServer {
        private TestWebsocketServer(InboundTrafficGuard guard, IRouter router) {
            super(HostAndPort.valueOf("127.0.0.1", 0), guard, router);
        }
        private int boundPort() { return ((java.net.InetSocketAddress) channelFuture.channel().localAddress()).getPort(); }
    }

    @Component
    public static final class ContractReceiver {
        private final ZfooSender sender = new ZfooSender(1024 * 1024, 128 * 1024);

        @PacketReceiver(Task.NettyIO)
        public void atTransportContractRequest(Session session, TransportContractRequest request, HttpAttachment attachment) {
            var response = new TransportContractResponse();
            response.setMarker(request.getMarker());
            response.setSuccess(true);
            sender.send(session, response, attachment);
        }
    }
}