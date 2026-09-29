package io.gameframe.storage.redis;

import io.gameframe.storage.*;
import io.lettuce.core.*;
import java.util.LinkedHashSet;
import java.util.Set;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.*;

/** Redis-specific primitives plus a small TTL-backed idempotency state machine. */
public final class RedisStore implements AutoCloseable {
    public enum IdempotencyStatus { ACQUIRED, IN_PROGRESS, COMPLETED, ABSENT }
    public record IdempotencyState(IdempotencyStatus status, String result) {
        public IdempotencyState {
            Objects.requireNonNull(status);
            if (status == IdempotencyStatus.COMPLETED) Objects.requireNonNull(result);
            else if (result != null) throw new IllegalArgumentException("result only exists for COMPLETED");
        }
    }
    public enum CompletionStatus { APPLIED, ALREADY_COMPLETED, REJECTED, MISSING }

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final OrderedExecutor executor;

    public RedisStore(String uri, int lanes, int capacity) {
        this(uri, lanes, capacity, true);
    }

    /**
     * Creates a store; autoReconnect controls whether Lettuce may reconnect and replay a command after a broken socket.
     * Production callers should keep it enabled and reconcile uncertain writes.
     */
    public RedisStore(String uri, int lanes, int capacity, boolean autoReconnect) {
        if (lanes < 1 || capacity < 1) throw new IllegalArgumentException();
        client = RedisClient.create(uri);
        client.setDefaultTimeout(Duration.ofSeconds(5));
        client.setOptions(ClientOptions.builder().autoReconnect(autoReconnect).requestQueueSize(capacity)
            .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS).build());
        try { connection = client.connect(); }
        catch (RuntimeException e) { client.shutdown(); throw e; }
        executor = new OrderedExecutor("redis-store", lanes, capacity);
    }
    private <T> CompletionStage<T> command(String key, Callable<T> work) {
        return executor.submit(key, () -> {
            try { return work.call(); }
            catch (RedisException e) {
                throw new StorageException(StorageException.Outcome.UNKNOWN,
                    "Redis result uncertain; do not blindly repeat increments", e);
            }
        });
    }
    public CompletionStage<String> get(String key) { return command(key, () -> connection.sync().get(key)); }

    /** Atomically replaces a value only when it still equals expected. */
    public CompletionStage<Boolean> compareAndSet(String key, String expected, String replacement, Duration ttl) {
        if (key == null || key.isBlank() || expected == null || replacement == null || replacement.isBlank()
                || ttl == null || ttl.toMillis() < 1) throw new IllegalArgumentException("invalid compare-and-set argument");
        return command(key, () -> {
            Long changed = connection.sync().eval(
                    "local v=redis.call('get',KEYS[1]); if v==ARGV[1] then redis.call('set',KEYS[1],ARGV[2],'PX',ARGV[3]); return 1 else return 0 end",
                    ScriptOutputType.INTEGER, new String[]{key}, expected, replacement, Long.toString(ttl.toMillis()));
            return changed == 1;
        });
    }

    public CompletionStage<Boolean> delete(String key) {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("key required");
        return command(key, () -> connection.sync().del(key) == 1);
    }
    /** Atomically deletes a value only when it still equals expected. */
    public CompletionStage<Boolean> compareAndDelete(String key, String expected) {
        if (key == null || key.isBlank() || expected == null) throw new IllegalArgumentException("invalid compare-and-delete argument");
        return command(key, () -> {
            Long removed = connection.sync().eval(
                    "local v=redis.call('get',KEYS[1]); if v==ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end",
                    ScriptOutputType.INTEGER, new String[]{key}, expected);
            return removed == 1;
        });
    }
    /** Bounded key discovery for control-plane namespaces; callers must provide a finite limit. */
    public CompletionStage<Set<String>> scanKeys(String pattern, int limit) {
        if (pattern == null || pattern.isBlank() || limit < 1) {
            throw new IllegalArgumentException("pattern and positive limit required");
        }
        return command(pattern, () -> {
            var keys = new LinkedHashSet<String>();
            var cursor = ScanCursor.INITIAL;
            var args = ScanArgs.Builder.matches(pattern).limit(Math.min(limit, 512));
            do {
                var page = connection.sync().scan(cursor, args);
                cursor = page;
                for (String key : page.getKeys()) {
                    keys.add(key);
                    if (keys.size() >= limit) break;
                }
            } while (!cursor.isFinished() && keys.size() < limit);
            return Set.copyOf(keys);
        });
    }

    public CompletionStage<String> set(String key, String value, Duration ttl) {
        long millis = ttl.toMillis(); if (millis < 1) throw new IllegalArgumentException("positive TTL required");
        return command(key, () -> connection.sync().set(key, value, SetArgs.Builder.px(millis)));
    }
    public CompletionStage<Boolean> setIfAbsent(String key, String value, Duration ttl) {
        long millis = ttl.toMillis(); if (millis < 1) throw new IllegalArgumentException("positive TTL required");
        return command(key, () -> "OK".equals(connection.sync().set(key, value, SetArgs.Builder.nx().px(millis))));
    }
    public CompletionStage<Long> increment(String key, long delta) { return command(key, () -> connection.sync().incrby(key, delta)); }

    /** Compare-and-renew updates a lease only for the current owner token. */
    public CompletionStage<Boolean> renewLease(String key, String token, String replacement, Duration ttl) {
        if (key == null || key.isBlank() || token == null || token.isBlank()
                || replacement == null || replacement.isBlank() || ttl == null || ttl.toMillis() < 1) {
            throw new IllegalArgumentException("invalid lease renewal argument");
        }
        return command(key, () -> {
            Long renewed = connection.sync().eval(
                    "local v=redis.call('get',KEYS[1]); if v and string.sub(v,1,string.len(ARGV[1])) == ARGV[1] then redis.call('set',KEYS[1],ARGV[2],'PX',ARGV[3]); return 1 else return 0 end",
                    ScriptOutputType.INTEGER, new String[]{key}, token, replacement, Long.toString(ttl.toMillis()));
            return renewed == 1;
        });
    }
    /** Compare-and-delete avoids deleting a lease acquired by a newer owner. */
    public CompletionStage<Boolean> releaseLease(String key, String token) {
        return command(key, () -> {
            Long removed = connection.sync().eval(
                "local v=redis.call('get',KEYS[1]); if v and string.sub(v,1,string.len(ARGV[1])) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end",
                ScriptOutputType.INTEGER, new String[]{key}, token);
            return removed == 1;
        });
    }

    /** Claims a durable operation slot. A completed result is returned without taking the slot again. */
    public CompletionStage<IdempotencyState> claimIdempotency(String key, String token, Duration ttl) {
        validateIdempotencyArgument(key, token, ttl);
        String encoded = encode(token);
        long millis = ttl.toMillis();
        return command(key, () -> {
            String reply = connection.sync().eval(CLAIM_SCRIPT, ScriptOutputType.VALUE,
                new String[]{key}, "P:" + encoded, Long.toString(millis));
            if (reply == null || reply.equals("A")) return new IdempotencyState(IdempotencyStatus.ACQUIRED, null);
            if (reply.equals("I")) return new IdempotencyState(IdempotencyStatus.IN_PROGRESS, null);
            if (reply.startsWith("C:")) return new IdempotencyState(IdempotencyStatus.COMPLETED, decode(reply.substring(2)));
            throw new IllegalStateException("invalid idempotency state");
        });
    }

    /** Commits a result only for the current token; a newer claimant cannot be overwritten. */
    public CompletionStage<CompletionStatus> completeIdempotency(
        String key, String token, String result, Duration ttl) {
        validateIdempotencyArgument(key, token, ttl);
        if (result == null || result.length() > 16_384) throw new IllegalArgumentException("invalid result");
        long millis = ttl.toMillis();
        return command(key, () -> {
            Long reply = connection.sync().eval(COMMIT_SCRIPT, ScriptOutputType.INTEGER, new String[]{key},
                "P:" + encode(token), "D:" + encode(result), Long.toString(millis));
            return switch (reply.intValue()) {
                case 1 -> CompletionStatus.APPLIED;
                case 2 -> CompletionStatus.ALREADY_COMPLETED;
                case 3 -> CompletionStatus.REJECTED;
                default -> CompletionStatus.MISSING;
            };
        });
    }

    /** Reads the current pending/completed marker; an expired marker is ABSENT. */
    public CompletionStage<IdempotencyState> getIdempotency(String key) {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("invalid idempotency key");
        return command(key, () -> parseState(connection.sync().get(key)));
    }

    private static final String CLAIM_SCRIPT =
        "local v=redis.call('get',KEYS[1]); " +
        "if not v then redis.call('set',KEYS[1],ARGV[1],'PX',ARGV[2]); return 'A' end; " +
        "if v==ARGV[1] then redis.call('pexpire',KEYS[1],ARGV[2]); return 'A' end; " +
        "if string.sub(v,1,2)=='D:' then return 'C:'..string.sub(v,3) end; return 'I'";
    private static final String COMMIT_SCRIPT =
        "local v=redis.call('get',KEYS[1]); if not v then return 0 end; " +
        "if v==ARGV[1] then redis.call('set',KEYS[1],ARGV[2],'PX',ARGV[3]); return 1 end; " +
        "if string.sub(v,1,2)=='D:' and v==ARGV[2] then return 2 end; " +
        "if string.sub(v,1,2)=='D:' then return 3 end; return 3";

    private static IdempotencyState parseState(String value) {
        if (value == null) return new IdempotencyState(IdempotencyStatus.ABSENT, null);
        if (value.startsWith("D:")) return new IdempotencyState(
            IdempotencyStatus.COMPLETED, decode(value.substring(2)));
        return new IdempotencyState(IdempotencyStatus.IN_PROGRESS, null);
    }
    private static void validateIdempotencyArgument(String key, String token, Duration ttl) {
        if (key == null || key.isBlank() || token == null || token.isBlank() ||
            token.length() > 256 || ttl == null || ttl.toMillis() < 1)
            throw new IllegalArgumentException("invalid idempotency argument");
    }
    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }
    public void close() { executor.close(); connection.close(); client.shutdown(); }
}
