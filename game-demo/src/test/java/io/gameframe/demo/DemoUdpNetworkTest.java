package io.gameframe.demo;

import com.zfoo.net.NetContext;
import com.zfoo.protocol.ProtocolManager;
import com.zfoo.net.anno.PacketReceiver;
import com.zfoo.net.anno.Task;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.router.attachment.UdpAttachment;
import com.zfoo.net.session.Session;
import io.gameframe.demo.protocol.PlayerRequest;
import io.gameframe.demo.protocol.PlayerResponse;
import io.gameframe.transport.GameUdpServer;
import io.gameframe.transport.InboundTrafficGuard;
import io.gameframe.transport.ReliableUdpChannelHandler;
import io.gameframe.transport.ReliableUdpFrame;
import io.gameframe.transport.ReliableUdpFrameCodec;
import io.gameframe.transport.ZfooSender;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.springframework.stereotype.Component;
import org.springframework.context.support.GenericXmlApplicationContext;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DemoUdpNetworkTest {
    @Test
    void plainAndReliableZfooUdpRoundTripThroughRealSockets() throws Exception {
        GenericXmlApplicationContext context = null;
        if (ProtocolManager.isProtocolClass(PlayerRequest.class)) {
            // DemoNetworkTest may have initialized zfoo already in this Surefire JVM.
            // ProtocolManager is intentionally process-singleton, so reuse its Router.
            // DemoReceiver is already registered by the earlier network test and accepts UdpAttachment.
        } else {
            context = new GenericXmlApplicationContext();
            context.registerBean(UdpContractReceiver.class, UdpContractReceiver::new);
            context.load("game-net.xml");
            context.refresh();
        }

        var plainServer = new TestUdpServer(null);
        var reliableServer = new TestUdpServer(ReliableUdpChannelHandler.Limits.defaults());
        try {
            plainServer.start();
            reliableServer.start();

            try (var plainSocket = new DatagramSocket();
                 var reliableSocket = new DatagramSocket()) {
                plainSocket.setSoTimeout(5000);
                reliableSocket.setSoTimeout(5000);

                byte[] plainRequest = encodeRequest("plain");
                plainSocket.send(new DatagramPacket(plainRequest, plainRequest.length,
                        new InetSocketAddress("127.0.0.1", plainServer.boundPort())));
                var plainResponse = receive(plainSocket);
                var plainDecoded = decodeZfooResponse(
                        plainResponse.getData(), plainResponse.getOffset(), plainResponse.getLength());
                assertTrue(plainDecoded.isSuccess(), plainDecoded.getMessage());
                assertEquals(5, plainDecoded.getGold());

                byte[] reliableRequest = encodeRequest("reliable");
                byte[] wireRequest = encodeReliable(
                        ReliableUdpFrame.data(1234, 1, 1, 0, 1, reliableRequest));
                reliableSocket.send(new DatagramPacket(wireRequest, wireRequest.length,
                        new InetSocketAddress("127.0.0.1", reliableServer.boundPort())));

                PlayerResponse reliableDecoded = null;
                ReliableUdpFrame responseFrame = null;
                boolean requestAcknowledged = false;
                for (int attempt = 0; attempt < 8 && reliableDecoded == null; attempt++) {
                    var datagram = receive(reliableSocket);
                    var frame = decodeReliable(datagram);
                    if (frame.kind() == ReliableUdpFrame.Kind.ACK) {
                        requestAcknowledged |= frame.sessionGeneration() == 1234 && frame.sequence() == 1;
                        continue;
                    }
                    if (frame.kind() == ReliableUdpFrame.Kind.HELLO) {
                        continue;
                    }
                    responseFrame = frame;
                    reliableDecoded = decodeZfooResponse(frame.payload(), 0, frame.payload().length);
                }
                assertTrue(requestAcknowledged, "server did not acknowledge reliable request");
                assertNotNull(responseFrame);
                assertNotNull(reliableDecoded);
                assertTrue(reliableDecoded.isSuccess(), reliableDecoded.getMessage());
                assertEquals(5, reliableDecoded.getGold());

                byte[] acknowledgement = encodeReliable(
                        ReliableUdpFrame.acknowledgement(responseFrame.sessionGeneration(), responseFrame.sequence()));
                reliableSocket.send(new DatagramPacket(acknowledgement, acknowledgement.length,
                        new InetSocketAddress("127.0.0.1", reliableServer.boundPort())));
            }
        } finally {
            reliableServer.shutdown();
            plainServer.shutdown();
            if (context != null) {
                context.close();
            }
        }
    }
    private static byte[] encodeRequest(String marker) {
        var request = new PlayerRequest();
        request.setRequestId(marker + "-" + UUID.randomUUID());
        request.setAction("read");
        request.setAmount(0);
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

    private static byte[] encodeReliable(ReliableUdpFrame frame) {
        ByteBuf encoded = ReliableUdpFrameCodec.encode(
                io.netty.buffer.UnpooledByteBufAllocator.DEFAULT, frame);
        try {
            byte[] bytes = new byte[encoded.readableBytes()];
            encoded.readBytes(bytes);
            return bytes;
        } finally {
            encoded.release();
        }
    }

    private static ReliableUdpFrame decodeReliable(DatagramPacket packet) {
        ByteBuf content = Unpooled.wrappedBuffer(packet.getData(), packet.getOffset(), packet.getLength());
        try {
            return ReliableUdpFrameCodec.decode(content);
        } finally {
            content.release();
        }
    }

    private static PlayerResponse decodeZfooResponse(byte[] bytes, int offset, int length) {
        ByteBuf encoded = Unpooled.wrappedBuffer(bytes, offset, length);
        try {
            int bodyLength = encoded.readInt();
            assertTrue(bodyLength > 0 && bodyLength <= encoded.readableBytes());
            return (PlayerResponse) NetContext.getPacketService()
                    .read(encoded.readSlice(bodyLength)).getPacket();
        } finally {
            encoded.release();
        }
    }

    private static DatagramPacket receive(DatagramSocket socket) throws Exception {
        var packet = new DatagramPacket(new byte[65_507], 65_507);
        socket.receive(packet);
        return packet;
    }

    private static final class TestUdpServer extends GameUdpServer {
        private TestUdpServer(ReliableUdpChannelHandler.Limits reliableLimits) {
            super(HostAndPort.valueOf("127.0.0.1", 0), guard(), NetContext.getRouter(), reliableLimits);
        }

        private int boundPort() {
            return ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }

        private static InboundTrafficGuard guard() {
            return new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                    64, 64 * 1024, Duration.ofSeconds(1), Duration.ofSeconds(30)));
        }
    }

    @Component
    public static final class UdpContractReceiver {
        private final ZfooSender sender = new ZfooSender(1024 * 1024, 128 * 1024);

        @PacketReceiver(Task.NettyIO)
        public void atPlayerRequest(Session session, PlayerRequest request, UdpAttachment attachment) {
            var response = new PlayerResponse();
            response.setRequestId(request.getRequestId());
            response.setSuccess(true);
            response.setMessage("ok");
            response.setVersion(1);
            response.setGold(5);
            sender.send(session, response, attachment);
        }
    }
}
