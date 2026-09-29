package io.gameframe.transport;

import com.zfoo.net.handler.ServerRouteHandler;
import com.zfoo.net.packet.DecodedPacketInfo;
import com.zfoo.net.router.IRouter;
import com.zfoo.net.util.SessionUtils;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

public final class GameServerRouteHandler extends ServerRouteHandler {
    private final IRouter router;
    private final ZfooRpcServerHandler rpcHandler;

    public GameServerRouteHandler(IRouter router) {
        this(router, null);
    }

    public GameServerRouteHandler(IRouter router, ZfooRpcServerHandler rpcHandler) {
        this.router = Objects.requireNonNull(router, "router");
        this.rpcHandler = rpcHandler;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        var session = SessionUtils.getSession(ctx);
        if (session == null) {
            return;
        }
        if (!(msg instanceof DecodedPacketInfo decodedPacketInfo)) {
            throw new IllegalArgumentException("expected DecodedPacketInfo but got " + msg.getClass().getName());
        }
        if (decodedPacketInfo.getPacket() instanceof RpcRequestEnvelope request && rpcHandler != null) {
            rpcHandler.dispatch(session, request, decodedPacketInfo.getAttachment());
            return;
        }
        router.receive(session, decodedPacketInfo.getPacket(), decodedPacketInfo.getAttachment());
    }
}