package io.gameframe.demo;

import org.junit.jupiter.api.Test;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class PackagedDemoIT {
    private String run(String... args) throws Exception {
        Path log = Files.createTempFile(Path.of("target"), "packaged-demo-", ".log");
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-jar", "target/game-demo-0.1.0-SNAPSHOT.jar"));
        command.addAll(List.of(args));
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().remove("GAME_MONGO_URI"); builder.environment().put("GAME_PORT", "0");
        Process process = builder.start();
        try {
            process.getOutputStream().write("\n".getBytes(StandardCharsets.UTF_8)); process.getOutputStream().close();
            boolean exited = process.waitFor(30, TimeUnit.SECONDS);
            if (!exited) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
            String output = Files.readString(log);
            assertTrue(exited, "packaged process leaked threads: " + output);
            assertEquals(0, process.exitValue(), output);
            assertFalse(output.contains("Exception thrown from ApplicationListener"), output);
            return output;
        } finally { if (process.isAlive()) process.destroyForcibly(); }
    }
    @Test void packagedPlayerSampleRuns() throws Exception {
        assertTrue(run().contains("Recovered: Snapshot[version=3"));
    }
    @Test void packagedTcpServerClosesAllZfooThreads() throws Exception {
        String output = run("--serve");
        assertTrue(output.contains("Development TCP endpoint"), output);
        assertTrue(output.contains("Net shutdown gracefully."), output);
    }
    @Test void packagedZfooClassesMatchLocalJdk25Build() throws Exception {
        Map<String, String> classes = Map.of(
            "protocol", "com/zfoo/protocol/ProtocolManager.class",
            "event", "com/zfoo/event/manager/EventBus.class",
            "scheduler", "com/zfoo/scheduler/SchedulerContext.class",
            "net", "com/zfoo/net/NetContext.class");
        try (var jar = new java.util.jar.JarFile("target/game-demo-0.1.0-SNAPSHOT.jar")) {
            for (var entry : classes.entrySet()) {
                String module = entry.getKey();
                byte[] local = Files.readAllBytes(Path.of("..", "vendor", "zfoo", module,
                    "target", "classes", entry.getValue()));
                assertEquals(69, java.nio.ByteBuffer.wrap(local).getShort(6), module + " must target Java 25");
                var classEntry = jar.getJarEntry(entry.getValue());
                assertNotNull(classEntry, module);
                try (var stream = jar.getInputStream(classEntry)) {
                    assertArrayEquals(local, stream.readAllBytes(), module + " must come from local source");
                }
                var metadata = jar.getJarEntry("META-INF/maven/com.zfoo/" + module + "/pom.properties");
                assertNotNull(metadata, module);
                var properties = new Properties();
                try (var stream = jar.getInputStream(metadata)) { properties.load(stream); }
                assertEquals("4.1.4-gameframe-SNAPSHOT", properties.getProperty("version"), module);
            }
            assertNotNull(jar.getJarEntry("META-INF/zfoo/LICENSE"));
            assertNull(jar.getJarEntry("com/zfoo/orm/OrmContext.class"));
        }
    }
    @Test void packagedSceneSampleRunsWithoutDatabases() throws Exception {
        String output = run("--scene");
        assertTrue(output.contains("Scene sample: Summary[movementTicks=4, passiveTicks=0"), output);
        assertTrue(output.contains("firstBatchEncoded=1, firstBatchReused=1, acceptedPackets=3"), output);
        assertTrue(output.contains("observer2Position=8.0, observer3Position=6.0"), output);
    }
    @Test void packagedZoneHandoffExitsAndCleansSubscriptions() throws Exception {
        String output = run("--zone");
        assertTrue(output.contains("Zone sample: Summary[outcome=ACCEPTED, sourceObjects=0, targetObjects=1, gold=108, subscriptionsAfterStop=0]"), output);
    }    @Test void packagedPersistenceSampleCoalescesAndFlushesBeforeExit() throws Exception {
        assertTrue(run("--persistence").contains(
            "Persistence sample: Summary[mergedEdits=100, pendingBeforePeriodic=2, writeCalls=6, coreVersion=4, bagVersion=2, gold=203, slots=4, dirtyAfterShutdown=0]"));
    }}
