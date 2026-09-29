package io.gameframe.transport;

import com.zfoo.protocol.ProtocolManager;
import com.zfoo.protocol.buffer.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Objects;

/** Encodes registered zfoo packets into the RPC envelope payload. */
public final class ZfooRpcCodec {
    private ZfooRpcCodec() {
    }

    public static RpcRequestEnvelope request(GameRpcClient.Request<?> request) {
        Objects.requireNonNull(request, "request");
        return new RpcRequestEnvelope(request.destination(), request.commandId(), request.deadlineMillis(), encode(request.payload()));
    }

    public static byte[] encode(Object packet) {
        Objects.requireNonNull(packet, "packet");
        ByteBuf buffer = Unpooled.buffer();
        try {
            ProtocolManager.write(buffer, packet);
            return ByteBufUtils.readAllBytes(buffer);
        } finally {
            buffer.release();
        }
    }

    public static Object decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
        try {
            Object packet = ProtocolManager.read(buffer);
            if (buffer.isReadable()) {
                throw new IllegalArgumentException("RPC payload has trailing bytes");
            }
            return packet;
        } finally {
            buffer.release();
        }
    }

    public static <T> T decode(byte[] bytes, Class<T> type) {
        Object packet = decode(bytes);
        if (!type.isInstance(packet)) {
            throw new IllegalArgumentException("RPC payload type mismatch: expected " + type.getName()
                    + " but got " + packet.getClass().getName());
        }
        return type.cast(packet);
    }
}