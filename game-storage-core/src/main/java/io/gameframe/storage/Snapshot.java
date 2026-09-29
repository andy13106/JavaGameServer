package io.gameframe.storage;
import java.util.Map;
public record Snapshot(long version, Map<String, Object> data) {
    public Snapshot { if (version < 0) throw new IllegalArgumentException(); data = Values.freeze(data); }
}
