package io.gameframe.transport;

import com.zfoo.net.core.AbstractServer;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.handler.codec.http.HttpCodecHandler;
import com.zfoo.net.packet.DecodedPacketInfo;
import com.zfoo.net.router.IRouter;
import com.zfoo.protocol.util.IOUtils;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Objects;
import java.util.function.Function;

public class GameHttpServer extends AbstractServer<SocketChannel> {
    private final InboundTrafficGuard trafficGuard;
    private final IRouter router;
    private final Function<FullHttpRequest, DecodedPacketInfo> uriResolver;
    private final ZfooRpcServerHandler rpcHandler;

    public GameHttpServer(HostAndPort host,
                          InboundTrafficGuard trafficGuard,
                          IRouter router,
                          Function<FullHttpRequest, DecodedPacketInfo> uriResolver) {
        this(host, trafficGuard, router, uriResolver, null);
    }

    public GameHttpServer(HostAndPort host,
                          InboundTrafficGuard trafficGuard,
                          IRouter router,
                          Function<FullHttpRequest, DecodedPacketInfo> uriResolver,
                          ZfooRpcServerHandler rpcHandler) {
        super(host);
        this.trafficGuard = Objects.requireNonNull(trafficGuard, "trafficGuard");
        this.router = Objects.requireNonNull(router, "router");
        this.uriResolver = Objects.requireNonNull(uriResolver, "uriResolver");
        this.rpcHandler = rpcHandler;
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        configurePipeline(channel);
    }

    void configurePipeline(Channel channel) {
        channel.pipeline().addLast("gameInboundBytes", new InboundByteLimitHandler(trafficGuard));
        channel.pipeline().addLast("zfooHttpCodec", new HttpServerCodec(
                8 * IOUtils.BYTES_PER_KB, 16 * IOUtils.BYTES_PER_KB, 16 * IOUtils.BYTES_PER_KB));
        channel.pipeline().addLast("zfooHttpAggregator",
                new HttpObjectAggregator(16 * IOUtils.BYTES_PER_MB));
        channel.pipeline().addLast("zfooChunkedWrite", new ChunkedWriteHandler());
        channel.pipeline().addLast("zfooHttpPacketCodec", new HttpCodecHandler(uriResolver));
        channel.pipeline().addLast("gameInboundPackets", new InboundPacketLimitHandler(trafficGuard));
        channel.pipeline().addLast("gameRoute", new GameServerRouteHandler(router, rpcHandler));
    }
}