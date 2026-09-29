package io.gameframe.demo;

import com.zfoo.net.NetContext;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.router.IRouter;
import com.zfoo.protocol.ProtocolManager;
import io.gameframe.demo.protocol.TransportContractRequest;
import io.gameframe.transport.GameTlsContext;
import io.gameframe.transport.GameWebsocketServer;
import io.gameframe.transport.InboundTrafficGuard;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericXmlApplicationContext;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;

class DemoTlsWebsocketNetworkTest {
    @Test
    void realWssClientCompletesTlsAndWebsocketHandshake() throws Exception {
        GenericXmlApplicationContext context = null;
        if (!ProtocolManager.isProtocolClass(TransportContractRequest.class)) {
            context = new GenericXmlApplicationContext();
            context.load("game-net.xml");
            context.refresh();
        }

        var certificate = new SelfSignedCertificate("localhost");
        SslContext tls = GameTlsContext.server(
                certificate.certificate().toPath(), certificate.privateKey().toPath());
        var server = new TestWebsocketServer(guard(), NetContext.getRouter(), tls);
        try {
            server.start();
            HttpClient client = HttpClient.newBuilder()
                    .sslContext(clientContext(certificate))
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            WebSocket websocket = client.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .buildAsync(URI.create("wss://localhost:" + server.boundPort() + "/websocket"),
                            new WebSocket.Listener() {
                                @Override
                                public void onOpen(WebSocket webSocket) {
                                    webSocket.request(1);
                                }
                            })
                    .get(5, TimeUnit.SECONDS);
            assertFalse(websocket.isOutputClosed());
            websocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
        } finally {
            server.shutdown();
            certificate.delete();
            if (context != null) context.close();
        }
    }

    private static SSLContext clientContext(SelfSignedCertificate certificate) throws Exception {
        var factory = CertificateFactory.getInstance("X.509");
        java.security.cert.Certificate trusted;
        try (var input = Files.newInputStream(certificate.certificate().toPath())) {
            trusted = factory.generateCertificate(input);
        }
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("gameframe-test-server", trusted);
        TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(keyStore);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustManagers.getTrustManagers(), new SecureRandom());
        return context;
    }

    private static InboundTrafficGuard guard() {
        return new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                64, 64 * 1024, Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }

    private static final class TestWebsocketServer extends GameWebsocketServer {
        private TestWebsocketServer(InboundTrafficGuard guard, IRouter router, SslContext tls) {
            super(HostAndPort.valueOf("127.0.0.1", 0), guard, router, tls);
        }

        private int boundPort() {
            return ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }
    }
}
