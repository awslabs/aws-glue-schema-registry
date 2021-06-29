package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class GlueSchemaRegistryCompressionFactoryTest {
    private final Map<String, Object> configs = new HashMap<>();
    private GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;
    GlueSchemaRegistryCompressionFactory glueSchemaRegistryCompressionFactory;

    @BeforeEach
    public void setup() {
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "User-Registry");
        glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        glueSchemaRegistryCompressionFactory = new GlueSchemaRegistryCompressionFactory();
    }

    @Test
    public void testConstructor_validSerdeConfigs_succeeds() {
        assertDoesNotThrow(() -> new GlueSchemaRegistryCompressionFactory());
    }

    @Test
    public void testGetCompressionHandler_nullCompressionType_returnsNull() {
        assertNull(glueSchemaRegistryCompressionFactory.getCompressionHandler(null));
    }

    @Test
    public void testGetCompressionHandler_noneCompressionType_returnsNull() {
        assertNull(glueSchemaRegistryCompressionFactory.getCompressionHandler(AWSSchemaRegistryConstants.COMPRESSION.NONE));
    }

    @Test
    public void testGetCompressionHandler_zlibCompressionTypeWithZLibCompressionNotInitialized_initializesUsingDefaultCompression() {
        GlueSchemaRegistryCompressionHandler
                glueSchemaRegistryCompressionHandler = glueSchemaRegistryCompressionFactory.getCompressionHandler(AWSSchemaRegistryConstants.COMPRESSION.ZLIB);
        assertEquals(GlueSchemaRegistryDefaultCompression.class, glueSchemaRegistryCompressionHandler.getClass());
    }

    @Test
    public void testGetCompressionHandler_zlibCompressionTypeWithZLibCompressionInitialized_initializesUsingDefaultCompression() {
        //Initialize call
        GlueSchemaRegistryCompressionHandler
                instance1 = glueSchemaRegistryCompressionFactory.getCompressionHandler(AWSSchemaRegistryConstants.COMPRESSION.ZLIB);
        //Return initialized instance.
        GlueSchemaRegistryCompressionHandler
                instance2 = glueSchemaRegistryCompressionFactory.getCompressionHandler(AWSSchemaRegistryConstants.COMPRESSION.ZLIB);
        assertEquals(instance1, instance2);
    }

    @Test
    public void testGetCompressionHandler_unknownCompressionByte_returnsNull() {
        byte compressionBytes = Byte.parseByte("1");
        assertNull(glueSchemaRegistryCompressionFactory.getCompressionHandler(compressionBytes));
    }

    @Test
    public void testGetCompressionHandler_knownCompressionByte_returnsNull() {
        assertEquals(GlueSchemaRegistryDefaultCompression.class, glueSchemaRegistryCompressionFactory.getCompressionHandler(AWSSchemaRegistryConstants.COMPRESSION_BYTE).getClass());
    }
}
