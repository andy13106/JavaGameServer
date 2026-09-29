package io.gameframe.transport;

import com.zfoo.net.core.AbstractServer;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.handler.codec.tcp.TcpCodecHandler;
import com.zfoo.net.router.IRouter;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import java.util.Objects;

public class GameTcpServer extends AbstractServer<SocketChannel> {
    private final InboundTrafficGuard trafficGuard;
    private final IRouter router;
    private final SslContext sslContext;
    private final ZfooRpcServerHandler rpcHandler;

    public GameTcpServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router) {
        this(host, trafficGuard, router, null, null);
    }

    public GameTcpServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router,
                         SslContext sslContext) {
        this(host, trafficGuard, router, sslContext, null);
    }

    public GameTcpServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router,
                         SslContext sslContext, ZfooRpcServerHandler rpcHandler) {
        super(host);
        this.trafficGuard = Objects.requireNonNull(trafficGuard, "trafficGuard");
        this.router = Objects.requireNonNull(router, "router");
        this.sslContext = sslContext;
        this.rpcHandler = rpcHandler;
        if (sslContext != null && !sslContext.isServer()) {
            throw new IllegalArgumentException("GameTcpServer requires a server SslContext");
        }
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        configurePipeline(channel);
    }

    void configurePipeline(Channel channel) {
        channel.pipeline().addLast("gameInboundBytes", new InboundByteLimitHandler(trafficGuard));
        if (sslContext != null) {
            channel.pipeline().addLast("gameTls", sslContext.newHandler(channel.alloc()));
        }
        channel.pipeline().addLast("zfooTcpCodec", new TcpCodecHandler());
        channel.pipeline().addLast("gameInboundPackets", new InboundPacketLimitHandler(trafficGuard));
        channel.pipeline().addLast("gameRoute", new GameServerRouteHandler(router, rpcHandler));
    }
}