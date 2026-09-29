package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SecretResolverTest {
    @Test
    void injectedSecretWinsAndReturnsDefensiveChars() {
        var resolver = new SecretResolver(Map.of("DB_PASSWORD", "change-me"), "GAMEFRAME_");
        assertEquals("change-me", resolver.required("DB_PASSWORD"));
        char[] chars = resolver.requiredChars("DB_PASSWORD");
        chars[0] = 'X';
        assertEquals("change-me", resolver.required("DB_PASSWORD"));
    }

    @Test
    void systemPropertyFallbackAndMissingErrorsDoNotRevealValues() {
        String name = "GAMEFRAME_TEST_SECRET";
        System.setProperty(name, "from-property");
        try {
            var resolver = new SecretResolver(Map.of());
            assertEquals("from-property", resolver.resolve(name).orElseThrow());
            var error = assertThrows(IllegalArgumentException.class,
                    () -> resolver.required("MISSING_SECRET"));
            assertEquals("missing secret: MISSING_SECRET", error.getMessage());
        } finally {
            System.clearProperty(name);
        }
    }

    @Test
    void validatesNamesAndPrefixesBeforeLookup() {
        assertThrows(IllegalArgumentException.class, () -> new SecretResolver(Map.of(), "bad-prefix-"));
        var resolver = new SecretResolver(Map.of());
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve("bad-name"));
        assertTrue(resolver.resolve("ABSENT_SECRET").isEmpty());
    }
}