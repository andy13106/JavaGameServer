package io.gameframe.transport;

import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.packet.DecodedPacketInfo;
import com.zfoo.net.router.Router;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GameHttpServerTest {
    @Test
    void buildsHttpPacketPipelineWithProtection() {
        var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                32, 4096, Duration.ofSeconds(1), Duration.ofSeconds(30)));
        var server = new ExposedGameHttpServer(
                HostAndPort.valueOf("127.0.0.1", 0), guard, new Router(), request -> (DecodedPacketInfo) null);
        var channel = new EmbeddedChannel();
        server.initialize(channel);

        assertNotNull(channel.pipeline().get("gameInboundBytes"));
        assertNotNull(channel.pipeline().get("zfooHttpCodec"));
        assertNotNull(channel.pipeline().get("zfooHttpAggregator"));
        assertNotNull(channel.pipeline().get("zfooHttpPacketCodec"));
        assertNotNull(channel.pipeline().get("gameInboundPackets"));
        assertEquals("gameInboundBytes", channel.pipeline().firstContext().name());
        channel.finishAndReleaseAll();
    }

    private static final class ExposedGameHttpServer extends GameHttpServer {
        private ExposedGameHttpServer(HostAndPort host,
                                      InboundTrafficGuard guard,
                                      Router router,
                                      java.util.function.Function<FullHttpRequest, DecodedPacketInfo> resolver) {
            super(host, guard, router, resolver);
        }

        private void initialize(io.netty.channel.Channel channel) {
            configurePipeline(channel);
        }
    }
}
