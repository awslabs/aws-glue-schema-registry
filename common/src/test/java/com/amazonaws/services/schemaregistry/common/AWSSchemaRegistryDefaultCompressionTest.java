package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;

public class AWSSchemaRegistryDefaultCompressionTest {
    private final Map<String, Object> configs = new HashMap<>();
    private GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;
    private static final byte[] testByteArray = {1, 2, 3};
    private AWSSchemaRegistryDefaultCompression awsSchemaRegistryDefaultCompression;

    @BeforeEach
    public void setup() {
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "User-Topic");
        glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryDefaultCompression = new AWSSchemaRegistryDefaultCompression();
    }

    @Test
    public void testCompress_invalidInput_throwsAWSSchemaRegistryException() {
        try {
            awsSchemaRegistryDefaultCompression.compress(null);
            fail("No exception was thrown.");
        } catch (Exception e) {
            assertEquals(AWSSchemaRegistryException.class, e.getClass());
            assertEquals("Error while compressing data", e.getMessage());
        }
    }

    @Test
    public void testCompress_byteArray_throwsAWSSchemaRegistryException() {
        assertDoesNotThrow(() -> awsSchemaRegistryDefaultCompression.compress(testByteArray));
    }

    @Test
    public void testDecompress_invalidInput_throwsAWSSchemaRegistryException() {
        try {
            awsSchemaRegistryDefaultCompression.decompress(testByteArray, 0, testByteArray.length);
            fail("No exception was thrown.");
        } catch (Exception e) {
            assertEquals(AWSSchemaRegistryException.class, e.getClass());
            assertEquals("Error while decompressing data", e.getMessage());
        }
    }

    @Test
    public void testDecompress_validInput_throwsAWSSchemaRegistryException() {
        byte[] compressedRecord = awsSchemaRegistryDefaultCompression.compress(testByteArray);
        assertDoesNotThrow(() -> awsSchemaRegistryDefaultCompression.decompress(compressedRecord, 0, compressedRecord.length));
    }
}
