package io.gameframe.demo;

import com.zfoo.net.NetContext;
import com.zfoo.net.core.HostAndPort;
import com.zfoo.net.router.IRouter;
import io.gameframe.transport.GameTcpServer;
import io.gameframe.transport.InboundTrafficGuard;
import io.gameframe.transport.PlayerMigrationCommand;
import io.gameframe.transport.ZfooPlayerMigrationHandler;
import io.gameframe.transport.ZfooRpcServerHandler;
import io.gameframe.transport.ZfooSender;
import org.springframework.context.support.GenericXmlApplicationContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/** Standalone Game node used by deployment smoke tests and cross-JVM migration exercises. */
public final class MigrationNodeMain {
    private MigrationNodeMain() {}

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        String name = option(args, "--name", "game-node");
        String host = option(args, "--host", "127.0.0.1");
        int port = Integer.parseInt(option(args, "--port", "9001"));
        try (var context = new GenericXmlApplicationContext()) {
            context.load("game-net.xml");
            context.refresh();
            var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
                    256, 4 * 1024 * 1024, Duration.ofSeconds(1), Duration.ofSeconds(30)));
            var migration = new ZfooPlayerMigrationHandler(command -> {
                System.out.println("MIGRATION_EVENT " + name + " " + operation(command.getOperation())
                        + " " + command.getPlayerId());
                return CompletableFuture.completedFuture(true);
            });
            var rpc = new ZfooRpcServerHandler(new ZfooSender(4 * 1024 * 1024, 512 * 1024), migration);
            var server = new NodeServer(HostAndPort.valueOf(host + ":" + port), guard,
                    NetContext.getRouter(), rpc);
            try {
                server.start();
                int boundPort = server.boundPort();
                System.out.println("MIGRATION_NODE_READY " + name + " " + boundPort);
                System.out.flush();
                new BufferedReader(new InputStreamReader(System.in)).readLine();
            } finally {
                server.shutdown();
                migration.close();
            }
        }
    }

    private static final class NodeServer extends GameTcpServer {
        private NodeServer(HostAndPort host, InboundTrafficGuard guard, IRouter router,
                           ZfooRpcServerHandler rpc) {
            super(host, guard, router, null, rpc);
        }
        private int boundPort() {
            return ((java.net.InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }
    }

    private static String option(String[] args, String name, String fallback) {
        return Arrays.stream(args).filter(value -> value.startsWith(name + "="))
                .map(value -> value.substring(name.length() + 1)).findFirst().orElse(fallback);
    }

    private static String operation(int value) {
        return switch (value) {
            case PlayerMigrationCommand.FREEZE -> "freeze";
            case PlayerMigrationCommand.RELEASE -> "release";
            case PlayerMigrationCommand.RESUME -> "resume";
            case PlayerMigrationCommand.PREPARE -> "prepare";
            case PlayerMigrationCommand.COMMIT -> "commit";
            case PlayerMigrationCommand.CANCEL -> "cancel";
            default -> "unknown-" + value;
        };
    }
}