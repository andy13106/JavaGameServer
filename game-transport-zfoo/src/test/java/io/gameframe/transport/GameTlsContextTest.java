package io.gameframe.transport;

import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.router.Router;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
class GameTlsContextTest {
    @Test
    void tlsContextIsInsertedAfterRawByteGuardForTcpAndWebsocket() throws Exception {
        var certificate = new SelfSignedCertificate("localhost");
        try {
            SslContext serverContext = SslContextBuilder.forServer(
                    certificate.certificate(), certificate.privateKey()).build();
            var guard = guard();

            var tcp = new ExposedTcp(guard, serverContext);
            var tcpChannel = new EmbeddedChannel();
            tcp.initialize(tcpChannel);
            assertNotNull(tcpChannel.pipeline().get("gameTls"));
            assertEquals("gameInboundBytes", tcpChannel.pipeline().firstContext().name());
            assertEquals("gameTls", tcpChannel.pipeline().names().get(1));
            tcpChannel.finishAndReleaseAll();

            var websocket = new ExposedWebsocket(guard, serverContext);
            var websocketChannel = new EmbeddedChannel();
            websocket.initialize(websocketChannel);
            assertNotNull(websocketChannel.pipeline().get("gameTls"));
            assertEquals("gameTls", websocketChannel.pipeline().names().get(1));
            websocketChannel.finishAndReleaseAll();
        } finally {
            certificate.delete();
        }
    }

    @Test
    void realTcpTlsHandshakeSucceedsAndRejectsWrongHostname() throws Exception {
        var serverCertificate = new SelfSignedCertificate("localhost");
        try {
            SslContext serverContext = GameTlsContext.server(serverCertificate.certificate().toPath(),
                    serverCertificate.privateKey().toPath());
            SslContext clientContext = GameTlsContext.client(serverCertificate.certificate().toPath());
            handshake(serverContext, clientContext, "localhost");
            assertThrows(Exception.class, () -> handshake(serverContext, clientContext, "wrong.example"));
        } finally {
            serverCertificate.delete();
        }
    }

    @Test
    void clientRejectsUntrustedServerCertificate() throws Exception {
        var serverCertificate = new SelfSignedCertificate("localhost");
        var unrelatedCertificate = new SelfSignedCertificate("unrelated");
        try {
            SslContext serverContext = GameTlsContext.server(serverCertificate.certificate().toPath(),
                    serverCertificate.privateKey().toPath());
            SslContext clientContext = GameTlsContext.client(unrelatedCertificate.certificate().toPath());
            assertThrows(Exception.class, () -> handshake(serverContext, clientContext, "localhost"));
        } finally {
            unrelatedCertificate.delete();
            serverCertificate.delete();
        }
    }

    @Test
    void mutualTlsRequiresAndAcceptsTrustedClientCertificate() throws Exception {
        var serverCertificate = new SelfSignedCertificate("localhost");
        var clientCertificate = new SelfSignedCertificate("game-client");
        try {
            SslContext serverContext = GameTlsContext.server(serverCertificate.certificate().toPath(),
                    serverCertificate.privateKey().toPath(), clientCertificate.certificate().toPath(), true);
            SslContext mutualClient = GameTlsContext.client(clientCertificate.certificate().toPath(),
                    clientCertificate.privateKey().toPath(), serverCertificate.certificate().toPath());
            handshake(serverContext, mutualClient, "localhost");

        } finally {
            clientCertificate.delete();
            serverCertificate.delete();
        }
    }

    @Test
    void buildersRejectIncompleteMutualTlsConfiguration() throws Exception {
        var certificate = new SelfSignedCertificate("localhost");
        try {
            assertThrows(IllegalArgumentException.class, () ->
                    GameTlsContext.server(certificate.certificate().toPath(),
                            certificate.privateKey().toPath(), null, true));
            assertThrows(IllegalArgumentException.class, () ->
                    GameTlsContext.client(certificate.certificate().toPath(), null,
                            certificate.certificate().toPath()));
        } finally {
            certificate.delete();
        }
    }

    private static void handshake(SslContext serverContext, SslContext clientContext,
                                  String peerHost) throws Exception {
        EventLoopGroup serverGroup = new NioEventLoopGroup(1);
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);
        Channel serverChannel = null;
        Channel clientChannel = null;
        try {
            serverChannel = new ServerBootstrap()
                    .group(serverGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline().addLast(serverContext.newHandler(channel.alloc()));
                        }
                    })
                    .bind("127.0.0.1", 0).sync().channel();
            int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
            var handler = new SslHandler[1];
            clientChannel = new Bootstrap()
                    .group(clientGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            handler[0] = GameTlsContext.clientHandler(clientContext, channel.alloc(), peerHost, port);
                            channel.pipeline().addLast(handler[0]);
                        }
                    })
                    .connect("127.0.0.1", port).sync().channel();
            handler[0].handshakeFuture().sync();
            assertTrue(handler[0].handshakeFuture().isSuccess());
        } finally {
            if (clientChannel != null) {
                clientChannel.close().syncUninterruptibly();
            }
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
            clientGroup.shutdownGracefully().syncUninterruptibly();
            serverGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static InboundTrafficGuard guard() {
        return new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                256, 1024 * 1024, Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }

    private static final class ExposedTcp extends GameTcpServer {
        private ExposedTcp(InboundTrafficGuard guard, SslContext context) {
            super(HostAndPort.valueOf("127.0.0.1", 0), guard, new Router(), context);
        }

        private void initialize(io.netty.channel.Channel channel) {
            configurePipeline(channel);
        }
    }

    private static final class ExposedWebsocket extends GameWebsocketServer {
        private ExposedWebsocket(InboundTrafficGuard guard, SslContext context) {
            super(HostAndPort.valueOf("127.0.0.1", 0), guard, new Router(), context);
        }

        private void initialize(io.netty.channel.Channel channel) {
            configurePipeline(channel);
        }

        private int boundPort() {
            return ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }
    }
}
