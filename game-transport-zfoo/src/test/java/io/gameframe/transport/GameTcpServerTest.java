package io.gameframe.transport;

import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.router.Router;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GameTcpServerTest {
    @Test
    void buildsProtectionPipelineInExpectedOrder() {
        var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(32, 1024,
                Duration.ofSeconds(1), Duration.ofSeconds(30)));
        var server = new ExposedGameTcpServer(HostAndPort.valueOf("127.0.0.1", 0), guard, new Router());
        var channel = new EmbeddedChannel();
        server.initialize(channel);

        assertNotNull(channel.pipeline().get("gameInboundBytes"));
        assertNotNull(channel.pipeline().get("zfooTcpCodec"));
        assertNotNull(channel.pipeline().get("gameInboundPackets"));
        assertNotNull(channel.pipeline().get("gameRoute"));
        assertEquals("gameInboundBytes", channel.pipeline().firstContext().name());
        channel.finishAndReleaseAll();
    }

    private static final class ExposedGameTcpServer extends GameTcpServer {
        private ExposedGameTcpServer(HostAndPort host, InboundTrafficGuard guard, Router router) {
            super(host, guard, router);
        }
        private void initialize(Channel channel) {
            configurePipeline(channel);
        }
    }
}
