package io.gameframe.transport;

import com.zfoo.net.NetContext;
import com.zfoo.net.handler.codec.http.HttpCodecHandler;
import com.zfoo.net.handler.codec.udp.UdpCodecHandler;
import com.zfoo.net.handler.codec.websocket.WebSocketCodecHandler;
import com.zfoo.net.packet.EncodedPacketInfo;
import com.zfoo.net.router.attachment.UdpAttachment;
import com.zfoo.net.session.Session;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public final class ZfooSender {
    private final OutboundPacketQueue outbound;

    public ZfooSender(long globalLimit, long sessionLimit) {
        this(globalLimit, sessionLimit, 4096);
    }

    public ZfooSender(long globalLimit, long sessionLimit, int sessionPacketLimit) {
        this.outbound = new OutboundPacketQueue(
                new OutboundPacketQueue.Limits(sessionPacketLimit, sessionLimit, globalLimit));
    }

    public CompletionStage<Void> send(Session session, Object packet) {
        return send(session, packet, null);
    }

    public CompletionStage<Void> send(Session session, Object packet, Object attachment) {
        var encoded = encode(session, packet, attachment);
        return outbound.send(session.getChannel(), encoded.message(), encoded.bytes())
                .thenApply(ignored -> null);
    }

    public CompletionStage<OutboundPacketQueue.SendOutcome> sendLatest(Session session,
                                                                       Object coalesceKey,
                                                                       Object packet) {
        return sendLatest(session, coalesceKey, packet, null);
    }

    public CompletionStage<OutboundPacketQueue.SendOutcome> sendLatest(Session session,
                                                                       Object coalesceKey,
                                                                       Object packet,
                                                                       Object attachment) {
        Objects.requireNonNull(coalesceKey, "coalesceKey");
        var encoded = encode(session, packet, attachment);
        return outbound.sendLatest(session.getChannel(), coalesceKey, encoded.message(), encoded.bytes());
    }

    public OutboundPacketQueue.Snapshot snapshot(Session session) {
        return outbound.snapshot(session.getChannel());
    }

    public long pendingBytes() {
        return outbound.globalPendingBytes();
    }

    private static EncodedMessage encode(Session session, Object packet, Object attachment) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(packet, "packet");
        var channel = session.getChannel();
        ByteBuf bytes = channel.alloc().buffer();
        try {
            NetContext.getPacketService().writeHeaderAndBody(bytes, packet, attachment);
            Object message;
            if (channel.pipeline().get(WebSocketCodecHandler.class) != null) {
                message = new BinaryWebSocketFrame(bytes);
            } else if (channel.pipeline().get(HttpCodecHandler.class) != null) {
                int encodedBytes = bytes.readableBytes();
                bytes.release();
                message = EncodedPacketInfo.valueOf(session.getSid(), session.getUid(), packet, attachment);
                return new EncodedMessage(message, encodedBytes);
            } else if (channel.pipeline().get(UdpCodecHandler.class) != null) {
                if (!(attachment instanceof UdpAttachment udpAttachment)) {
                    throw new IllegalArgumentException("UDP sends require a UdpAttachment");
                }
                message = new DatagramPacket(bytes,
                        new InetSocketAddress(udpAttachment.getHost(), udpAttachment.getPort()));
            } else {
                message = bytes;
            }
            return new EncodedMessage(message, bytes.readableBytes());
        } catch (Throwable error) {
            bytes.release();
            throw error;
        }
    }

    private record EncodedMessage(Object message, int bytes) {
    }
}