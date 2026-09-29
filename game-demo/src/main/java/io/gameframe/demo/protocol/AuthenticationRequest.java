package io.gameframe.demo.protocol;

import com.zfoo.protocol.anno.Protocol;

@Protocol(id = 20005)
public class AuthenticationRequest {
    private String identity;
    private long timestampMillis;
    private String nonce;
    private String secret;

    public String getIdentity() { return identity; }
    public void setIdentity(String identity) { this.identity = identity; }
    public long getTimestampMillis() { return timestampMillis; }
    public void setTimestampMillis(long timestampMillis) { this.timestampMillis = timestampMillis; }
    public String getNonce() { return nonce; }
    public void setNonce(String nonce) { this.nonce = nonce; }
    public String getSecret() { return secret; }
    public void setSecret(String secret) { this.secret = secret; }
}