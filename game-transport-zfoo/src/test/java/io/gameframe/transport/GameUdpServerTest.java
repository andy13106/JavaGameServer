package io.gameframe.transport;

import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.router.Router;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class GameUdpServerTest {
    @Test
    void buildsUdpPipelineWithProtectionAndGameRoute() {
        var server = new ExposedGameUdpServer(guard(), null);
        var channel = new EmbeddedChannel();
        server.initialize(channel);

        assertNotNull(channel.pipeline().get("gameInboundBytes"));
        assertNull(channel.pipeline().get("gameReliableUdp"));
        assertNotNull(channel.pipeline().get("zfooUdpCodec"));
        assertNotNull(channel.pipeline().get("gameInboundPackets"));
        assertNotNull(channel.pipeline().get("gameRoute"));
        assertEquals("gameInboundBytes", channel.pipeline().firstContext().name());
        channel.finishAndReleaseAll();
    }

    @Test
    void explicitlyAddsReliableHandlerBeforeZfooCodec() {
        var server = new ExposedGameUdpServer(guard(), ReliableUdpChannelHandler.Limits.defaults());
        var channel = new EmbeddedChannel();
        server.initialize(channel);

        assertNotNull(channel.pipeline().get("gameReliableUdp"));
        assertEquals("gameInboundBytes", channel.pipeline().firstContext().name());
        var names = channel.pipeline().names();
        assertEquals(names.indexOf("gameReliableUdp") + 1, names.indexOf("zfooUdpCodec"));
        channel.finishAndReleaseAll();
    }

    private static InboundTrafficGuard guard() {
        return new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                64, 8192, Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }

    private static final class ExposedGameUdpServer extends GameUdpServer {
        private ExposedGameUdpServer(InboundTrafficGuard guard,
                                     ReliableUdpChannelHandler.Limits reliableLimits) {
            super(HostAndPort.valueOf("127.0.0.1", 0), guard, new Router(), reliableLimits);
        }

        private void initialize(io.netty.channel.Channel channel) {
            configurePipeline(channel);
        }
    }
}
