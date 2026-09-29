package io.gameframe.transport;

/** Raised when a reliable UDP send cannot be admitted to its bounded peer window. */
public final class ReliableUdpSendRejectedException extends RuntimeException {
    public ReliableUdpSendRejectedException(String message) {
        super(message);
    }
}
