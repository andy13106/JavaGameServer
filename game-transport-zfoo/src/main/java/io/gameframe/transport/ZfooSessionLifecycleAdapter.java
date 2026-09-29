package io.gameframe.transport;

import com.zfoo.event.anno.Bus;
import com.zfoo.event.enhance.IEventReceiver;
import com.zfoo.event.manager.EventBus;
import com.zfoo.event.model.IEvent;
import com.zfoo.net.core.event.ServerSessionActiveEvent;
import com.zfoo.net.core.event.ServerSessionInactiveEvent;
import com.zfoo.net.session.Session;

import java.util.Objects;
import java.util.function.Function;

/**
 * Connects zfoo server session events to {@link GameSessionRouter}.
 *
 * <p>The adapter is intentionally explicit and removable: it registers two
 * current-thread EventBus receivers and unregisters them on {@link #close()}.
 * Packet receivers should still call {@link GameSessionRouter#dispatch(Session,
 * java.util.concurrent.Callable)} so packet work enters the same actor mailbox.</p>
 */
public final class ZfooSessionLifecycleAdapter implements AutoCloseable {
    private final GameSessionRouter router;
    private final Function<Session, String> actorIdResolver;
    private final IEventReceiver activeReceiver;
    private final IEventReceiver inactiveReceiver;
    private boolean closed;

    public ZfooSessionLifecycleAdapter(GameSessionRouter router, Function<Session, String> actorIdResolver) {
        this.router = Objects.requireNonNull(router);
        this.actorIdResolver = Objects.requireNonNull(actorIdResolver);
        this.activeReceiver = receiver(ServerSessionActiveEvent.class, this::onActive);
        this.inactiveReceiver = receiver(ServerSessionInactiveEvent.class, this::onInactive);
        EventBus.registerEventReceiver(ServerSessionActiveEvent.class, activeReceiver);
        EventBus.registerEventReceiver(ServerSessionInactiveEvent.class, inactiveReceiver);
    }

    /** Uses uid when available and sid for pre-authenticated sessions. */
    public static ZfooSessionLifecycleAdapter byUid(GameSessionRouter router) {
        return new ZfooSessionLifecycleAdapter(router, session ->
            session.getUid() > 0 ? "uid:" + session.getUid() : "sid:" + session.getSid());
    }

    private IEventReceiver receiver(Class<? extends IEvent> eventType,
                                    java.util.function.Consumer<Session> consumer) {
        return new IEventReceiver() {
            @Override public Bus bus() { return Bus.CurrentThread; }
            @Override public void invoke(IEvent event) {
                Session session = eventType == ServerSessionActiveEvent.class
                    ? ((ServerSessionActiveEvent) event).getSession()
                    : ((ServerSessionInactiveEvent) event).getSession();
                consumer.accept(session);
            }
            @Override public Object getBean() { return ZfooSessionLifecycleAdapter.this; }
        };
    }

    private void onActive(Session session) {
        if (router.binding(session).isEmpty()) {
            router.bind(session, actorIdResolver.apply(session));
        }
    }

    private void onInactive(Session session) {
        router.disconnect(session);
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        EventBus.unregisterEventReceiver(ServerSessionActiveEvent.class, activeReceiver);
        EventBus.unregisterEventReceiver(ServerSessionInactiveEvent.class, inactiveReceiver);
    }
}

