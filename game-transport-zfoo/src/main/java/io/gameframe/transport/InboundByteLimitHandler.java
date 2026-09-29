package io.gameframe.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.AddressedEnvelope;
import io.netty.util.ReferenceCountUtil;

import java.util.Objects;

/**
 * Place before the zfoo protocol decoder to enforce the raw inbound byte budget.
 */
@ChannelHandler.Sharable
public final class InboundByteLimitHandler extends ChannelInboundHandlerAdapter {
    private final InboundTrafficGuard guard;

    public InboundByteLimitHandler(InboundTrafficGuard guard) {
        this.guard = Objects.requireNonNull(guard);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        int bytes = readableBytes(message);
        if (bytes > 0 && !guard.admitBytes(ctx.channel(), bytes, guard.nowNanos())) {
            ReferenceCountUtil.release(message);
            ctx.close();
            return;
        }
        ctx.fireChannelRead(message);
    }

    private static int readableBytes(Object message) {
        if (message instanceof ByteBuf buffer) return buffer.readableBytes();
        if (message instanceof ByteBufHolder holder) return holder.content().readableBytes();
        if (message instanceof AddressedEnvelope<?, ?> envelope && envelope.content() instanceof ByteBuf buffer)
            return buffer.readableBytes();
        return 0;
    }
}

