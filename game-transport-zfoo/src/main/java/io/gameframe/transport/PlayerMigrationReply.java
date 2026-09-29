package io.gameframe.transport;

import com.zfoo.protocol.anno.Protocol;

/** Correlated result for a player migration operation. */
@Protocol(id = 31005)
public class PlayerMigrationReply {
    private boolean accepted;
    private String message;

    public PlayerMigrationReply() {}
    public PlayerMigrationReply(boolean accepted, String message) {
        this.accepted = accepted;
        this.message = message;
    }
    public boolean isAccepted() { return accepted; }
    public void setAccepted(boolean accepted) { this.accepted = accepted; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}