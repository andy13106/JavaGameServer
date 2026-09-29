package io.gameframe.transport;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

/** Builds explicit TLS contexts for game listeners and trusted clients. */
public final class GameTlsContext {
    private GameTlsContext() {
    }

    public static SslContext server(Path certificate, Path privateKey) {
        return server(certificate, privateKey, null, false);
    }

    public static SslContext server(Path certificate, Path privateKey,
                                    Path trustCertificate, boolean requireClientAuth) {
        Objects.requireNonNull(certificate, "certificate");
        Objects.requireNonNull(privateKey, "privateKey");
        if (requireClientAuth && trustCertificate == null) {
            throw new IllegalArgumentException("mTLS requires a trust certificate");
        }
        try {
            SslContextBuilder builder = SslContextBuilder.forServer(file(certificate), file(privateKey));
            if (trustCertificate != null) {
                builder.trustManager(file(trustCertificate));
            }
            if (requireClientAuth) {
                builder.clientAuth(ClientAuth.REQUIRE);
            }
            return builder.build();
        } catch (Exception error) {
            throw new IllegalArgumentException("cannot build server TLS context", error);
        }
    }

    public static SslContext client(Path trustCertificate) {
        return client(null, null, trustCertificate);
    }

    public static SslContext client(Path certificate, Path privateKey, Path trustCertificate) {
        Objects.requireNonNull(trustCertificate, "trustCertificate");
        if ((certificate == null) != (privateKey == null)) {
            throw new IllegalArgumentException("client certificate and private key must be configured together");
        }
        try {
            SslContextBuilder builder = SslContextBuilder.forClient().trustManager(file(trustCertificate));
            if (certificate != null) {
                builder.keyManager(file(certificate), file(privateKey));
            }
            return builder.build();
        } catch (Exception error) {
            throw new IllegalArgumentException("cannot build client TLS context", error);
        }
    }

    /** Creates a client handler with HTTPS-style endpoint identification enabled. */
    public static SslHandler clientHandler(SslContext context, ByteBufAllocator allocator,
                                           String peerHost, int peerPort) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(allocator, "allocator");
        if (context.isServer()) {
            throw new IllegalArgumentException("client handler requires a client SslContext");
        }
        if (peerHost == null || peerHost.isBlank()) {
            throw new IllegalArgumentException("peerHost must not be blank");
        }
        SslHandler handler = context.newHandler(allocator, peerHost, peerPort);
        var parameters = handler.engine().getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        handler.engine().setSSLParameters(parameters);
        return handler;
    }

    private static File file(Path path) {
        return path.toAbsolutePath().normalize().toFile();
    }
}
