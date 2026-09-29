package io.gameframe.demo.protocol;

import com.zfoo.protocol.anno.Protocol;

@Protocol(id = 20006)
public class AuthenticationResponse {
    private boolean accepted;
    private String rejection;
    private long uid;

    public boolean isAccepted() { return accepted; }
    public void setAccepted(boolean accepted) { this.accepted = accepted; }
    public String getRejection() { return rejection; }
    public void setRejection(String rejection) { this.rejection = rejection; }
    public long getUid() { return uid; }
    public void setUid(long uid) { this.uid = uid; }
}