package io.gameframe.transport;

import com.zfoo.net.packet.DecodedPacketInfo;
import com.zfoo.net.router.IRouter;
import com.zfoo.net.router.attachment.UdpAttachment;
import com.zfoo.net.session.Session;
import com.zfoo.net.handler.BaseRouteHandler;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Routes UDP packets through one logical Session per remote address. */
public final class GameUdpRouteHandler extends BaseRouteHandler {
    public record Limits(int maxSessions, Duration idleTimeout) {
        public Limits {
            Objects.requireNonNull(idleTimeout, "idleTimeout");
            if (maxSessions < 1 || idleTimeout.isZero() || idleTimeout.isNegative()) {
                throw new IllegalArgumentException("invalid UDP session limits");
            }
        }

        public static Limits defaults() {
            return new Limits(16_384, Duration.ofMinutes(2));
        }
    }

    private final IRouter router;
    private final Limits limits;
    private final Map<InetSocketAddress, Entry> sessions = new LinkedHashMap<>();
    private long rejectedSessions;

    public GameUdpRouteHandler(IRouter router) {
        this(router, Limits.defaults());
    }

    public GameUdpRouteHandler(IRouter router, Limits limits) {
        this.router = Objects.requireNonNull(router, "router");
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof DecodedPacketInfo decodedPacketInfo)) {
            throw new IllegalArgumentException("expected DecodedPacketInfo but got " + msg.getClass().getName());
        }
        if (!(decodedPacketInfo.getAttachment() instanceof UdpAttachment attachment)) {
            throw new IllegalArgumentException("UDP packet is missing UdpAttachment");
        }
        long now = System.nanoTime();
        InetSocketAddress remote = new InetSocketAddress(attachment.getHost(), attachment.getPort());
        Entry entry = sessions.get(remote);
        if (entry == null) {
            evictIdle(now);
            if (sessions.size() >= limits.maxSessions()) {
                rejectedSessions++;
                return;
            }
            entry = new Entry(new Session(ctx.channel()), now);
            sessions.put(remote, entry);
        } else {
            entry.lastTouchedNanos = now;
        }
        router.receive(entry.session, decodedPacketInfo.getPacket(), attachment);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        sessions.clear();
        super.channelInactive(ctx);
    }

    public int sessionCount() {
        return sessions.size();
    }

    public long rejectedSessions() {
        return rejectedSessions;
    }

    Session sessionFor(InetSocketAddress remote) {
        var entry = sessions.get(remote);
        return entry == null ? null : entry.session;
    }

    private void evictIdle(long now) {
        long timeout = limits.idleTimeout().toNanos();
        Iterator<Map.Entry<InetSocketAddress, Entry>> iterator = sessions.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next().getValue();
            if (now - entry.lastTouchedNanos >= timeout) {
                iterator.remove();
            }
        }
    }

    private static final class Entry {
        private final Session session;
        private long lastTouchedNanos;

        private Entry(Session session, long lastTouchedNanos) {
            this.session = session;
            this.lastTouchedNanos = lastTouchedNanos;
        }
    }
}