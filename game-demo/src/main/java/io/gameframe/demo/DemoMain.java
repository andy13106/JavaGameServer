package io.gameframe.demo;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.core.tcp.TcpServer;
import io.gameframe.runtime.ActorSystem;
import io.gameframe.storage.*;
import io.gameframe.storage.mongo.MongoDocumentStore;
import org.springframework.context.support.GenericXmlApplicationContext;
import java.util.*;
import java.util.concurrent.*;

public final class DemoMain {
    public static void main(String[] args) throws Exception {
        if (Arrays.asList(args).contains("--benchmark")) {
            BenchmarkMain.runFromArgs(args);
            return;
        }
        if (Arrays.asList(args).contains("--migration-node")) {
            MigrationNodeMain.run(args);
            return;
        }
        if (Arrays.asList(args).contains("--cluster")) {
            System.out.println("Cluster scaling sample: " + ClusterScalingDemo.run());
            return;
        }
        if (Arrays.asList(args).contains("--scene")) {
            System.out.println("Scene sample: " + SceneDemo.run());
            return;
        }
        if (Arrays.asList(args).contains("--zone")) {
            System.out.println("Zone sample: " + ZoneDemo.run());
            return;
        }
        if (Arrays.asList(args).contains("--persistence")) {
            System.out.println("Persistence sample: " + PersistenceDemo.run());
            return;
        }
        String uri = System.getenv("GAME_MONGO_URI");
        try (var actors = new ActorSystem(2, 1000, 256, 32);
             DocumentStore store = uri == null || uri.isBlank() ? new MemoryDocumentStore(2, 256)
                : new MongoDocumentStore(uri, System.getenv().getOrDefault("GAME_MONGO_DATABASE", "gameframe_demo"), 2, 256)) {
            var player = new PlayerService(actors, store, "demo-player");
            System.out.println("Loaded: " + player.open().toCompletableFuture().get(20, TimeUnit.SECONDS));
            if (Arrays.asList(args).contains("--serve")) {
                try (var context = new GenericXmlApplicationContext()) {
                    context.registerBean(DemoReceiver.class, () -> new DemoReceiver(player));
                    context.load("game-net.xml"); context.refresh();
                    int port = Integer.parseInt(System.getenv().getOrDefault("GAME_PORT", "9000"));
                    String host = System.getenv().getOrDefault("GAME_BIND_HOST", "127.0.0.1");
                    new TcpServer(HostAndPort.valueOf(host + ":" + port)).start();
                    System.out.println("Development TCP endpoint: " + host + ":" + port + "; press Enter to stop.");
                    System.in.read();
                }
            } else {
                String op = UUID.randomUUID().toString();
                System.out.println("Patch: " + player.patch(op, new DocumentPatch(Map.of("quest.progress", 3L), Set.of(), Map.of("gold", 20L))).toCompletableFuture().get());
                var snapshot = player.read().toCompletableFuture().get().orElseThrow();
                System.out.println("Snapshot: " + player.replace(UUID.randomUUID().toString(), snapshot).toCompletableFuture().get());
                System.out.println("Recovered: " + player.read().toCompletableFuture().get().orElseThrow());
            }
        }
    }
}