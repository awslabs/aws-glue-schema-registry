package com.amazonaws.services.schemaregistry;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for reading configuration files.
 */
public class ConfigurationFileReader {

    /**
     * Loads configuration from a properties file.
     *
     * @param filePath Path to the properties file
     * @return Map containing the configuration key-value pairs
     * @throws IOException if the file cannot be read
     */
    public static Map<String, String> loadConfigFromFile(String filePath) throws IOException {
        Properties properties = new Properties();
        Map<String, String> configMap = new HashMap<>();

        try (FileInputStream input = new FileInputStream(filePath)) {
            properties.load(input);

            // Convert Properties to Map<String, String>
            for (String key : properties.stringPropertyNames()) {
                configMap.put(key, properties.getProperty(key));
            }
        }

        return configMap;
    }
}
