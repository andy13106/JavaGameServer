package io.gameframe.transport;

import com.zfoo.event.manager.EventBus;
import com.zfoo.net.anno.PacketReceiver;
import com.zfoo.net.core.event.ServerSessionActiveEvent;
import com.zfoo.net.core.event.ServerSessionInactiveEvent;
import com.zfoo.net.session.Session;
import com.zfoo.protocol.ProtocolManager;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class ActorAwareZfooRouterTest {
    @BeforeAll
    static void initProtocol() {
        ProtocolManager.initProtocol(Set.of(ActorRoutePacket.class, RpcRequestEnvelope.class, RpcResponseEnvelope.class, RpcPayload.class, PlayerMigrationCommand.class, PlayerMigrationReply.class));
    }

    @Test
    void receiveInvokesBoundPacketReceiverInsideActorSerially() throws Exception {
        var received = new Receiver(2);
        var dropped = new AtomicInteger();
        try (var sessions = new GameSessionRouter(1, 4, 8, 1);
             var lifecycle = ZfooSessionLifecycleAdapter.byUid(sessions)) {
            var router = new ActorAwareZfooRouter(sessions, (task, error) -> dropped.incrementAndGet());
            router.registerPacketReceiverDefinition(received);
            var channel = new EmbeddedChannel();
            try {
                Session session = new Session(501, 77, channel);
                EventBus.post(ServerSessionActiveEvent.valueOf(session));

                router.receive(session, new ActorRoutePacket(1), null);
                router.receive(session, new ActorRoutePacket(2), null);

                assertTrue(received.done.await(2, TimeUnit.SECONDS));
                assertEquals(java.util.List.of(1, 2), received.order);
                assertEquals(1, received.maxConcurrent.get());
                assertNotEquals(Thread.currentThread(), received.firstThread);

                EventBus.post(ServerSessionInactiveEvent.valueOf(session));
                router.receive(session, new ActorRoutePacket(3), null);
                assertEquals(1, dropped.get());
                assertEquals(2, received.order.size());
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    public static final class Receiver {
        private final CountDownLatch done;
        private final CopyOnWriteArrayList<Integer> order = new CopyOnWriteArrayList<>();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();
        private volatile Thread firstThread;

        public Receiver(int expected) {
            done = new CountDownLatch(expected);
        }

        @PacketReceiver
        public void atActorRoutePacket(Session session, ActorRoutePacket packet) {
            firstThread = firstThread == null ? Thread.currentThread() : firstThread;
            int now = active.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            try {
                order.add(packet.getSequence());
                Thread.sleep(20);
                done.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                active.decrementAndGet();
            }
        }
    }
}

