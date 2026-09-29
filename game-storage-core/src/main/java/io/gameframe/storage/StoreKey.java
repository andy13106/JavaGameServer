package io.gameframe.storage;
public record StoreKey(String section, String id) {
    public StoreKey {
        if (section == null || !section.matches("[a-z][a-z0-9_]{0,63}") || id == null || id.isBlank())
            throw new IllegalArgumentException("invalid storage key");
    }
}
