package io.gameframe.transport;

import com.zfoo.net.packet.DecodedPacketInfo;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Place after a zfoo decoder and before ServerRouteHandler.
 * Counts complete decoded packets and closes sessions that stop producing decoded traffic.
 */
@ChannelHandler.Sharable
public final class InboundPacketLimitHandler extends ChannelInboundHandlerAdapter {
    private static final AtomicLong IDS = new AtomicLong();

    private final InboundTrafficGuard guard;
    private final AttributeKey<ScheduledFuture<?>> timeoutKey =
        AttributeKey.valueOf("gameframe.inboundTimeout." + IDS.incrementAndGet());

    public InboundPacketLimitHandler(InboundTrafficGuard guard) {
        this.guard = Objects.requireNonNull(guard);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        guard.nanosUntilHeartbeatTimeout(ctx.channel(), guard.nowNanos());
        scheduleHeartbeatCheck(ctx, guard.limits().heartbeatTimeout().toNanos());
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        if (message instanceof DecodedPacketInfo
            && !guard.admitPacket(ctx.channel(), guard.nowNanos())) {
            ReferenceCountUtil.release(message);
            ctx.close();
            return;
        }
        ctx.fireChannelRead(message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ScheduledFuture<?> timeout = ctx.channel().attr(timeoutKey).getAndSet(null);
        if (timeout != null) timeout.cancel(false);
        super.channelInactive(ctx);
    }

    private void scheduleHeartbeatCheck(ChannelHandlerContext ctx, long delayNanos) {
        long safeDelay = Math.max(1, delayNanos);
        ScheduledFuture<?> future = ctx.executor().schedule(() -> {
            if (!ctx.channel().isActive()) return;
            long now = guard.nowNanos();
            long remaining = guard.nanosUntilHeartbeatTimeout(ctx.channel(), now);
            if (remaining <= 0 && guard.markHeartbeatTimeout(ctx.channel(), now)) {
                ctx.close();
                return;
            }
            scheduleHeartbeatCheck(ctx, Math.max(1, remaining));
        }, safeDelay, TimeUnit.NANOSECONDS);
        ctx.channel().attr(timeoutKey).set(future);
    }
}

