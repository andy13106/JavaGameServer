package io.gameframe.runtime;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Small bounded metrics registry for runtime and transport adapters.
 *
 * <p>Metric names are intentionally explicit and bounded so a request id or
 * player id cannot accidentally create unbounded cardinality. The snapshot is
 * immutable and sorted, which makes it suitable for tests and exporters.</p>
 */
public final class RuntimeMetrics {
    public record Snapshot(Map<String, Long> counters, Map<String, Long> gauges) {
        public Snapshot {
            counters = immutableSorted(counters, "counters");
            gauges = immutableSorted(gauges, "gauges");
        }

        private static Map<String, Long> immutableSorted(Map<String, Long> values, String name) {
            Objects.requireNonNull(values, name);
            var sorted = new TreeMap<String, Long>();
            values.forEach((key, value) -> {
                if (key == null || key.isBlank() || value == null) throw new IllegalArgumentException(name + " contains invalid entry");
                sorted.put(key, value);
            });
            return Collections.unmodifiableMap(sorted);
        }
    }

    private final int maxMetrics;
    private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> names = new ConcurrentHashMap<>();

    public RuntimeMetrics() { this(512); }

    public RuntimeMetrics(int maxMetrics) {
        if (maxMetrics < 1) throw new IllegalArgumentException("maxMetrics must be positive");
        this.maxMetrics = maxMetrics;
    }

    public void increment(String name) { increment(name, 1); }

    public void increment(String name, long delta) {
        requireName(name);
        var counter = counters.get(name);
        if (counter == null) {
            reserve(name);
            counter = counters.computeIfAbsent(name, ignored -> new LongAdder());
        }
        counter.add(delta);
    }

    public void gauge(String name, long value) {
        requireName(name);
        if (!gauges.containsKey(name)) reserve(name);
        gauges.computeIfAbsent(name, ignored -> new AtomicLong()).set(value);
    }

    public Snapshot snapshot() {
        var counterValues = new TreeMap<String, Long>();
        counters.forEach((name, value) -> counterValues.put(name, value.sum()));
        var gaugeValues = new TreeMap<String, Long>();
        gauges.forEach((name, value) -> gaugeValues.put(name, value.get()));
        return new Snapshot(counterValues, gaugeValues);
    }

    /** Returns a simple Prometheus exposition without timestamps. */
    public String toPrometheus() {
        var snapshot = snapshot();
        var output = new StringBuilder();
        snapshot.counters().forEach((name, value) -> output.append(name).append("_total ").append(value).append('\n'));
        snapshot.gauges().forEach((name, value) -> output.append(name).append(' ').append(value).append('\n'));
        return output.toString();
    }

    private void reserve(String name) {
        if (names.putIfAbsent(name, Boolean.TRUE) == null && names.size() > maxMetrics) {
            names.remove(name);
            throw new IllegalStateException("metrics capacity exceeded: " + maxMetrics);
        }
    }

    private static void requireName(String name) {
        if (name == null || name.isBlank() || !name.matches("[a-zA-Z_:][a-zA-Z0-9_:]*")) {
            throw new IllegalArgumentException("invalid metric name: " + name);
        }
    }
}