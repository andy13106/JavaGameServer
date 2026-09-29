package io.gameframe.transport;

import com.zfoo.event.manager.EventBus;
import com.zfoo.net.core.event.ServerSessionActiveEvent;
import com.zfoo.net.core.event.ServerSessionInactiveEvent;
import com.zfoo.net.session.Session;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ZfooSessionLifecycleAdapterTest {
    @Test
    void zfooSessionEventsBindAndUnbindActor() throws Exception {
        try (var router = new GameSessionRouter(1, 4, 4, 1);
             var adapter = ZfooSessionLifecycleAdapter.byUid(router)) {
            var channel = new EmbeddedChannel();
            try {
                Session session = new Session(401, 42, channel);
                EventBus.post(ServerSessionActiveEvent.valueOf(session));
                assertEquals(1, router.boundCount());
                assertEquals("uid:42", router.binding(session).orElseThrow().actorId());
                assertEquals(42, router.dispatch(session, session::getUid)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS));

                EventBus.post(ServerSessionInactiveEvent.valueOf(session));
                assertEquals(0, router.boundCount());
                assertThrows(ExecutionException.class, () -> router.dispatch(session, () -> 1)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS));
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void closedAdapterStopsReceivingEvents() {
        try (var router = new GameSessionRouter(1, 4, 4, 1)) {
            var adapter = ZfooSessionLifecycleAdapter.byUid(router);
            adapter.close();
            var channel = new EmbeddedChannel();
            try {
                Session session = new Session(402, 0, channel);
                EventBus.post(ServerSessionActiveEvent.valueOf(session));
                assertEquals(0, router.boundCount());
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }
}

