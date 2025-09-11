package com.amazonaws.services.schemaregistry.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurationFileReaderTest {

    private static final String REGION_KEY = "region";
    private static final String ROLE_TO_ASSUME_KEY = "roleToAssume";
    private static final String ROLE_SESSION_NAME_KEY = "roleSessionName";
    
    private static final String REGION_VALUE = "us-east-1";
    private static final String ROLE_ARN_VALUE = "arn:aws:iam::123456789012:role/TestRole";
    private static final String SESSION_NAME_VALUE = "custom-session";
    
    private static final String KEY_WITH_DOTS = "key.with.dots";
    private static final String KEY_WITH_UNDERSCORES = "key_with_underscores";
    private static final String KEY_WITH_COLONS = "key:with:colons";
    
    private static final String VALUE_WITH_SPACES = "value with spaces";
    private static final String VALUE_WITH_DASHES = "value-with-dashes";
    private static final String VALUE_WITH_EQUALS = "value=with=equals";

    @TempDir
    Path tempDir;

    @Test
    void testLoadConfigFromFile() throws IOException {
        // Create a temporary properties file
        Path propertiesFile = tempDir.resolve("test.properties");
        try (FileWriter writer = new FileWriter(propertiesFile.toFile())) {
            writer.write(REGION_KEY + "=" + REGION_VALUE + "\n");
            writer.write(ROLE_TO_ASSUME_KEY + "=" + ROLE_ARN_VALUE + "\n");
            writer.write(ROLE_SESSION_NAME_KEY + "=" + SESSION_NAME_VALUE + "\n");
        }

        Map<String, String> config = ConfigurationFileReader.loadConfigFromFile(propertiesFile.toString());

        assertEquals(REGION_VALUE, config.get(REGION_KEY));
        assertEquals(ROLE_ARN_VALUE, config.get(ROLE_TO_ASSUME_KEY));
        assertEquals(SESSION_NAME_VALUE, config.get(ROLE_SESSION_NAME_KEY));
        assertEquals(3, config.size());
    }

    @Test
    void testLoadConfigFromEmptyFile() throws IOException {
        // Create an empty properties file
        Path propertiesFile = tempDir.resolve("empty.properties");
        Files.createFile(propertiesFile);

        Map<String, String> config = ConfigurationFileReader.loadConfigFromFile(propertiesFile.toString());

        assertTrue(config.isEmpty());
    }

    @Test
    void testLoadConfigFromNonExistentFile() {
        String nonExistentFile = tempDir.resolve("nonexistent.properties").toString();

        assertThrows(IOException.class, () -> {
            ConfigurationFileReader.loadConfigFromFile(nonExistentFile);
        });
    }

    @Test
    void testLoadConfigHandlesSpecialCharacters() throws IOException {
        // Create a properties file with special characters
        Path propertiesFile = tempDir.resolve("special.properties");
        try (FileWriter writer = new FileWriter(propertiesFile.toFile())) {
            writer.write(KEY_WITH_DOTS + "=" + VALUE_WITH_SPACES + "\n");
            writer.write(KEY_WITH_UNDERSCORES + "=" + VALUE_WITH_DASHES + "\n");
            writer.write("key\\:with\\:colons=value\\=with\\=equals\n");
        }

        Map<String, String> config = ConfigurationFileReader.loadConfigFromFile(propertiesFile.toString());

        assertEquals(VALUE_WITH_SPACES, config.get(KEY_WITH_DOTS));
        assertEquals(VALUE_WITH_DASHES, config.get(KEY_WITH_UNDERSCORES));
        assertEquals(VALUE_WITH_EQUALS, config.get(KEY_WITH_COLONS));
    }
}
