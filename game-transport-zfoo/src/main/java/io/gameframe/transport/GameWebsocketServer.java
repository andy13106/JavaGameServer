package io.gameframe.transport;

import com.zfoo.net.core.AbstractServer;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.handler.codec.websocket.WebSocketCodecHandler;
import com.zfoo.net.router.IRouter;
import com.zfoo.protocol.util.IOUtils;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Objects;

public class GameWebsocketServer extends AbstractServer<SocketChannel> {
    private final InboundTrafficGuard trafficGuard;
    private final IRouter router;
    private final SslContext sslContext;
    private final ZfooRpcServerHandler rpcHandler;

    public GameWebsocketServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router) {
        this(host, trafficGuard, router, null, null);
    }

    public GameWebsocketServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router,
                               SslContext sslContext) {
        this(host, trafficGuard, router, sslContext, null);
    }

    public GameWebsocketServer(HostAndPort host, InboundTrafficGuard trafficGuard, IRouter router,
                               SslContext sslContext, ZfooRpcServerHandler rpcHandler) {
        super(host);
        this.trafficGuard = Objects.requireNonNull(trafficGuard, "trafficGuard");
        this.router = Objects.requireNonNull(router, "router");
        this.sslContext = sslContext;
        this.rpcHandler = rpcHandler;
        if (sslContext != null && !sslContext.isServer()) {
            throw new IllegalArgumentException("GameWebsocketServer requires a server SslContext");
        }
    }

    @Override
    public void initChannel(SocketChannel channel) {
        configurePipeline(channel);
    }

    void configurePipeline(Channel channel) {
        channel.pipeline().addLast("gameInboundBytes", new InboundByteLimitHandler(trafficGuard));
        if (sslContext != null) {
            channel.pipeline().addLast("gameTls", sslContext.newHandler(channel.alloc()));
        }
        channel.pipeline().addLast("zfooHttpCodec", new HttpServerCodec(
                8 * IOUtils.BYTES_PER_KB, 16 * IOUtils.BYTES_PER_KB, 16 * IOUtils.BYTES_PER_KB));
        channel.pipeline().addLast("zfooHttpAggregator", new HttpObjectAggregator(16 * IOUtils.BYTES_PER_MB));
        channel.pipeline().addLast("zfooWebsocketProtocol", new WebSocketServerProtocolHandler("/websocket"));
        channel.pipeline().addLast("zfooChunkedWrite", new ChunkedWriteHandler());
        channel.pipeline().addLast("zfooWebsocketCodec", new WebSocketCodecHandler());
        channel.pipeline().addLast("gameInboundPackets", new InboundPacketLimitHandler(trafficGuard));
        channel.pipeline().addLast("gameRoute", new GameServerRouteHandler(router, rpcHandler));
    }
}