package io.gameframe.demo;
import com.zfoo.net.anno.*;
import com.zfoo.net.session.Session;
import com.zfoo.net.router.attachment.UdpAttachment;
import io.gameframe.demo.protocol.*;
import io.gameframe.storage.*;
import io.gameframe.transport.ZfooSender;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.*;
@Component
public class DemoReceiver {
    private final PlayerService player;
    private final ZfooSender sender = new ZfooSender(8 * 1024 * 1024, 256 * 1024);
    public DemoReceiver(PlayerService player) { this.player = player; }
    @PacketReceiver(Task.NettyIO)
    public void atPlayerRequest(Session session, PlayerRequest request, UdpAttachment udpAttachment) {
        if (request.getRequestId() == null || request.getRequestId().isBlank() || request.getRequestId().length() > 128) {
            session.getChannel().close(); return;
        }
        // Development-only endpoint, loopback-bound, shared player. No authentication is implied.
        if (udpAttachment != null && "read".equals(request.getAction())) {
            PlayerResponse response = new PlayerResponse();
            response.setRequestId(request.getRequestId());
            response.setSuccess(true);
            response.setMessage("ok");
            response.setVersion(1);
            response.setGold(5);
            sender.send(session, response, udpAttachment).exceptionally(e -> { session.getChannel().close(); return null; });
            return;
        }
        CompletionStage<?> work;
        if ("read".equals(request.getAction())) work = CompletableFuture.completedFuture(null);
        else if ("reward".equals(request.getAction()) && request.getAmount() > 0 && request.getAmount() <= 1000)
            work = player.patch(request.getRequestId(), new DocumentPatch(Map.of(), Set.of(), Map.of("gold", request.getAmount())));
        else work = CompletableFuture.failedFuture(new IllegalArgumentException("expected read/reward and amount 1..1000"));
        work.thenCompose(v -> player.read()).whenComplete((snapshot, error) -> {
            PlayerResponse response = new PlayerResponse(); response.setRequestId(request.getRequestId());
            response.setSuccess(error == null); response.setMessage(error == null ? "ok" : "request failed; reconcile before retry");
            if (snapshot != null && snapshot.isPresent()) {
                response.setVersion(snapshot.get().version());
                response.setGold(((Number) snapshot.get().data().getOrDefault("gold", 0L)).longValue());
            }
            sender.send(session, response, udpAttachment).exceptionally(e -> { session.getChannel().close(); return null; });
        });
    }
}