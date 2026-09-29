package io.gameframe.transport;

import com.zfoo.net.packet.DecodedPacketInfo;
import com.zfoo.net.router.Router;
import com.zfoo.net.router.attachment.UdpAttachment;
import com.zfoo.net.session.Session;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GameUdpRouteHandlerTest {
    @Test
    void isolatesLogicalSessionByRemoteAddressAndReusesIt() {
        var received = new ArrayList<Session>();
        var router = new Router() {
            @Override
            public void receive(Session session, Object packet, Object attachment) {
                received.add(session);
            }
        };
        var handler = new GameUdpRouteHandler(router, new GameUdpRouteHandler.Limits(8, Duration.ofMinutes(1)));
        var channel = new EmbeddedChannel(handler);
        try {
            var first = UdpAttachment.valueOf("127.0.0.1", 10001);
            var second = UdpAttachment.valueOf("127.0.0.1", 10002);
            channel.writeInbound(DecodedPacketInfo.valueOf("one", first));
            channel.writeInbound(DecodedPacketInfo.valueOf("two", first));
            channel.writeInbound(DecodedPacketInfo.valueOf("three", second));

            assertEquals(3, received.size());
            assertEquals(received.get(0), received.get(1));
            assertNotEquals(received.get(0), received.get(2));
            assertEquals(2, handler.sessionCount());
            assertNotNull(handler.sessionFor(new InetSocketAddress("127.0.0.1", 10001)));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void enforcesRemoteSessionCapacityWithoutClosingSharedSocket() {
        var router = new Router() {
            @Override
            public void receive(Session session, Object packet, Object attachment) {
            }
        };
        var handler = new GameUdpRouteHandler(router, new GameUdpRouteHandler.Limits(1, Duration.ofMinutes(1)));
        var channel = new EmbeddedChannel(handler);
        try {
            channel.writeInbound(DecodedPacketInfo.valueOf("one", UdpAttachment.valueOf("127.0.0.1", 10001)));
            channel.writeInbound(DecodedPacketInfo.valueOf("two", UdpAttachment.valueOf("127.0.0.1", 10002)));
            assertEquals(1, handler.sessionCount());
            assertEquals(1, handler.rejectedSessions());
            assertEquals(true, channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}