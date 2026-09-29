package io.gameframe.storage;
public final class StorageException extends RuntimeException {
    public enum Outcome { NOT_EXECUTED, UNKNOWN }
    private final Outcome outcome;
    public StorageException(Outcome outcome, String message, Throwable cause) { super(message, cause); this.outcome = outcome; }
    public Outcome outcome() { return outcome; }
}
