package io.gameframe.storage;
public record WriteResult(Status status, long version) {
    public enum Status { APPLIED, ALREADY_APPLIED, CONFLICT, MISSING }
    public boolean successful() { return status == Status.APPLIED || status == Status.ALREADY_APPLIED; }
}
