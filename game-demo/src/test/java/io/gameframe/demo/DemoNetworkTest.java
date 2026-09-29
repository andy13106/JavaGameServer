package io.gameframe.demo;
import com.zfoo.net.NetContext;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.core.tcp.TcpServer;
import io.gameframe.demo.protocol.*;
import io.gameframe.runtime.ActorSystem;
import io.gameframe.storage.MemoryDocumentStore;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericXmlApplicationContext;
import java.io.*;
import java.net.*;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class DemoNetworkTest {
    static final class EphemeralServer extends TcpServer {
        EphemeralServer() { super(HostAndPort.valueOf("127.0.0.1:0")); }
        int boundPort() { return ((InetSocketAddress) channelFuture.channel().localAddress()).getPort(); }
    }
    @Test void tcpPacketReachesActorAndStoresPatchBeforeReply() throws Exception {
        try (var actors = new ActorSystem(2, 2, 32, 4); var store = new MemoryDocumentStore(2, 32)) {
            var player = new PlayerService(actors, store, "network-test"); player.open().toCompletableFuture().get(3, TimeUnit.SECONDS);
            try (var context = new GenericXmlApplicationContext()) {
                context.registerBean(DemoReceiver.class, () -> new DemoReceiver(player)); context.load("game-net.xml"); context.refresh();
                var server = new EphemeralServer(); server.start();
                try (var socket = new Socket("127.0.0.1", server.boundPort())) {
                    socket.setSoTimeout(5000);
                    var request = new PlayerRequest(); request.setRequestId(UUID.randomUUID().toString()); request.setAction("reward"); request.setAmount(5);
                    var encoded = Unpooled.buffer();
                    try {
                        NetContext.getPacketService().writeHeaderAndBody(encoded, request, null);
                        byte[] bytes = new byte[encoded.readableBytes()]; encoded.readBytes(bytes); socket.getOutputStream().write(bytes);
                    } finally { encoded.release(); }
                    var input = new DataInputStream(socket.getInputStream()); int length = input.readInt();
                    assertTrue(length > 0 && length < 1024 * 1024);
                    byte[] bytes = input.readNBytes(length); assertEquals(length, bytes.length);
                    var decoded = Unpooled.wrappedBuffer(bytes);
                    try {
                        var response = (PlayerResponse) NetContext.getPacketService().read(decoded).getPacket();
                        assertTrue(response.isSuccess(), response.getMessage()); assertEquals(105, response.getGold());
                        assertEquals(2, response.getVersion()); assertEquals(request.getRequestId(), response.getRequestId());
                    } finally { decoded.release(); }
                }
            }
        }
    }
}
