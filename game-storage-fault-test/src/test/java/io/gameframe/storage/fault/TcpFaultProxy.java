package io.gameframe.storage.fault;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Test-only TCP proxy that can drop or delay one server response after the request reached the target. */
public final class TcpFaultProxy implements AutoCloseable {
    private final String targetHost;
    private final int targetPort;
    private final ServerSocket listener;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean dropNextResponse = new AtomicBoolean();
    private final AtomicReference<Duration> stallNextResponse = new AtomicReference<>();
    private final AtomicBoolean responseDropped = new AtomicBoolean();
    private final AtomicBoolean responseStalled = new AtomicBoolean();
    private volatile boolean closed;

    public TcpFaultProxy(String targetHost, int targetPort) throws IOException {
        if (targetHost == null || targetHost.isBlank() || targetPort < 1 || targetPort > 65535)
            throw new IllegalArgumentException("invalid proxy target");
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        listener = new ServerSocket(0, 32, InetAddress.getLoopbackAddress());
        workers.submit(this::acceptLoop);
    }

    public int port() { return listener.getLocalPort(); }

    public void armDropNextServerResponse() {
        responseDropped.set(false);
        stallNextResponse.set(null);
        dropNextResponse.set(true);
    }

    public void armStallNextServerResponse(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero())
            throw new IllegalArgumentException("positive stall duration required");
        responseStalled.set(false);
        dropNextResponse.set(false);
        stallNextResponse.set(duration);
    }

    public boolean awaitResponseDropped(Duration timeout) throws InterruptedException {
        return awaitFlag(responseDropped, timeout);
    }

    public boolean awaitResponseStalled(Duration timeout) throws InterruptedException {
        return awaitFlag(responseStalled, timeout);
    }

    private static boolean awaitFlag(AtomicBoolean flag, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!flag.get() && System.nanoTime() < deadline) Thread.sleep(10);
        return flag.get();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = listener.accept();
                workers.submit(() -> bridge(client));
            } catch (IOException e) {
                if (!closed) break;
            }
        }
    }

    private void bridge(Socket client) {
        Socket target = null;
        try {
            client.setTcpNoDelay(true);
            target = new Socket();
            target.setTcpNoDelay(true);
            target.connect(new InetSocketAddress(targetHost, targetPort), 5000);
            Socket targetSocket = target;
            workers.submit(() -> pump(client, targetSocket, false));
            workers.submit(() -> pump(targetSocket, client, true));
        } catch (IOException e) {
            closeQuietly(client);
            closeQuietly(target);
        }
    }

    private void pump(Socket source, Socket destination, boolean serverToClient) {
        try (source; destination) {
            InputStream input = source.getInputStream();
            OutputStream output = destination.getOutputStream();
            byte[] buffer = new byte[16 * 1024];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read == 0) continue;
                if (serverToClient && dropNextResponse.compareAndSet(true, false)) {
                    responseDropped.set(true);
                    return;
                }
                if (serverToClient) {
                    Duration stall = stallNextResponse.getAndSet(null);
                    if (stall != null) {
                        responseStalled.set(true);
                        try { Thread.sleep(stall); }
                        catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                output.write(buffer, 0, read);
                output.flush();
            }
        } catch (IOException ignored) {
            // Closing either side is the fault injection behavior.
        }
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null) return;
        try { closeable.close(); } catch (IOException ignored) { }
    }

    @Override
    public void close() {
        closed = true;
        closeQuietly(listener);
        workers.close();
    }
}
