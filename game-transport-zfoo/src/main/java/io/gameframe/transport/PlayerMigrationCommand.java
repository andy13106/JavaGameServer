package io.gameframe.transport;

import com.zfoo.protocol.anno.Protocol;

/** Wire command for idempotent cross-process player migration operations. */
@Protocol(id = 31004)
public class PlayerMigrationCommand {
    public static final int FREEZE = 1;
    public static final int RELEASE = 2;
    public static final int RESUME = 3;
    public static final int PREPARE = 4;
    public static final int COMMIT = 5;
    public static final int CANCEL = 6;

    private int operation;
    private String playerId;
    private String sourceInstance;
    private String targetInstance;
    private long fencingToken;
    private long deadlineMillis;
    private byte[] snapshot;

    public PlayerMigrationCommand() {}

    public PlayerMigrationCommand(int operation, String playerId, String sourceInstance, String targetInstance,
                                  long fencingToken, long deadlineMillis, byte[] snapshot) {
        this.operation = operation;
        this.playerId = playerId;
        this.sourceInstance = sourceInstance;
        this.targetInstance = targetInstance;
        this.fencingToken = fencingToken;
        this.deadlineMillis = deadlineMillis;
        this.snapshot = snapshot == null ? null : snapshot.clone();
    }

    public int getOperation() { return operation; }
    public void setOperation(int operation) { this.operation = operation; }
    public String getPlayerId() { return playerId; }
    public void setPlayerId(String playerId) { this.playerId = playerId; }
    public String getSourceInstance() { return sourceInstance; }
    public void setSourceInstance(String sourceInstance) { this.sourceInstance = sourceInstance; }
    public String getTargetInstance() { return targetInstance; }
    public void setTargetInstance(String targetInstance) { this.targetInstance = targetInstance; }
    public long getFencingToken() { return fencingToken; }
    public void setFencingToken(long fencingToken) { this.fencingToken = fencingToken; }
    public long getDeadlineMillis() { return deadlineMillis; }
    public void setDeadlineMillis(long deadlineMillis) { this.deadlineMillis = deadlineMillis; }
    public byte[] getSnapshot() { return snapshot == null ? null : snapshot.clone(); }
    public void setSnapshot(byte[] snapshot) { this.snapshot = snapshot == null ? null : snapshot.clone(); }
}