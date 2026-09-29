package io.gameframe.transport;

import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.core.udp.UdpServer;
import com.zfoo.net.handler.codec.udp.UdpCodecHandler;
import com.zfoo.net.router.IRouter;
import io.netty.channel.Channel;

import java.util.Objects;

public class GameUdpServer extends UdpServer {
    private final InboundTrafficGuard trafficGuard;
    private final IRouter router;
    private final ReliableUdpChannelHandler.Limits reliableLimits;
    private final GameUdpRouteHandler.Limits sessionLimits;

    public GameUdpServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router) {
        this(host, trafficGuard, router, null, GameUdpRouteHandler.Limits.defaults());
    }

    public GameUdpServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router,
                         ReliableUdpChannelHandler.Limits reliableLimits) {
        this(host, trafficGuard, router, reliableLimits, GameUdpRouteHandler.Limits.defaults());
    }

    public GameUdpServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router,
                         ReliableUdpChannelHandler.Limits reliableLimits,
                         GameUdpRouteHandler.Limits sessionLimits) {
        super(host);
        this.trafficGuard = Objects.requireNonNull(trafficGuard, "trafficGuard");
        this.router = Objects.requireNonNull(router, "router");
        this.reliableLimits = reliableLimits;
        this.sessionLimits = Objects.requireNonNull(sessionLimits, "sessionLimits");
    }

    @Override
    protected void initChannel(Channel channel) {
        configurePipeline(channel);
    }

    void configurePipeline(Channel channel) {
        channel.pipeline().addLast("gameInboundBytes", new InboundByteLimitHandler(trafficGuard));
        if (reliableLimits != null) {
            channel.pipeline().addLast("gameReliableUdp", new ReliableUdpChannelHandler(reliableLimits));
        }
        channel.pipeline().addLast("zfooUdpCodec", new UdpCodecHandler());
        channel.pipeline().addLast("gameInboundPackets", new InboundPacketLimitHandler(trafficGuard));
        channel.pipeline().addLast("gameRoute", new GameUdpRouteHandler(router, sessionLimits));
    }
}