package io.gameframe.runtime;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Resolves deployment secrets without putting their values into configuration records or errors. */
public final class SecretResolver {
    private final Map<String, String> injected;
    private final String environmentPrefix;

    public SecretResolver(Map<String, String> injected) { this(injected, ""); }

    public SecretResolver(Map<String, String> injected, String environmentPrefix) {
        Objects.requireNonNull(injected, "injected");
        if (environmentPrefix == null || !environmentPrefix.matches("[A-Za-z_][A-Za-z0-9_]*|")) {
            throw new IllegalArgumentException("invalid environment prefix");
        }
        this.injected = Map.copyOf(injected);
        this.environmentPrefix = environmentPrefix;
    }

    /** Looks up injected values first, then environment variables, then system properties. */
    public Optional<String> resolve(String name) {
        requireName(name);
        String value = injected.get(name);
        if (isPresent(value)) return Optional.of(value);
        value = System.getenv(environmentPrefix + name);
        if (isPresent(value)) return Optional.of(value);
        value = System.getProperty(name);
        return isPresent(value) ? Optional.of(value) : Optional.empty();
    }

    public String required(String name) {
        return resolve(name).orElseThrow(() -> new IllegalArgumentException("missing secret: " + name));
    }

    /** Returns a defensive character copy for APIs that accept char[] passwords. */
    public char[] requiredChars(String name) { return required(name).toCharArray(); }

    private static boolean isPresent(String value) { return value != null && !value.isBlank(); }
    private static void requireName(String name) {
        if (name == null || name.isBlank() || !name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("invalid secret name");
        }
    }
}