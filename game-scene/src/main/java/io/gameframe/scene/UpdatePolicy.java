package io.gameframe.scene;

import java.time.Duration;
import java.util.Objects;

/** Intervals are simulation nanoseconds for INTERVAL, update calls for EVERY_N_TICKS. */
public record UpdatePolicy(Mode mode, long interval) {
    public enum Mode { PASSIVE, EVERY_FRAME, EVERY_N_TICKS, INTERVAL }
    public UpdatePolicy {
        Objects.requireNonNull(mode);
        if ((mode == Mode.PASSIVE && interval != 0)
            || (mode == Mode.EVERY_FRAME && interval != 1)
            || ((mode == Mode.EVERY_N_TICKS || mode == Mode.INTERVAL) && interval < 1))
            throw new IllegalArgumentException("invalid update interval");
    }
    public static UpdatePolicy passive() { return new UpdatePolicy(Mode.PASSIVE, 0); }
    public static UpdatePolicy everyFrame() { return new UpdatePolicy(Mode.EVERY_FRAME, 1); }
    public static UpdatePolicy everyTicks(long ticks) { return new UpdatePolicy(Mode.EVERY_N_TICKS, ticks); }
    public static UpdatePolicy interval(Duration duration) { return new UpdatePolicy(Mode.INTERVAL, duration.toNanos()); }
    public boolean active() { return mode != Mode.PASSIVE; }
}
