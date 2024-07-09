package com.amazonaws.services.schemaregistry.common;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AWSSerializerInputTest {
    private final String SCHEMA_NAME = "test-schema-name";
    private final String TRANSPORT_NAME = "test-transport-name";
    private final String DEFAULT_TRANSPORT_NAME = "default-stream";

    @Test
    public void testBuilder_withSchemaNameAndTransportName_objectBuildSuccessfully() {
        AWSSerializerInput awsSerializerInput = AWSSerializerInput.builder()
                .transportName(TRANSPORT_NAME)
                .schemaName(SCHEMA_NAME)
                .build();

        assertEquals(SCHEMA_NAME, awsSerializerInput.getSchemaName());
        assertEquals(TRANSPORT_NAME, awsSerializerInput.getTransportName());
    }

    @Test
    public void testBuilder_withNullSchemaNameAndNullTransportName_objectBuildSuccessfully() {
        AWSSerializerInput awsSerializerInput = AWSSerializerInput.builder()
                .transportName(null)
                .schemaName(null)
                .build();

        assertNull(awsSerializerInput.getSchemaName());
        assertEquals(DEFAULT_TRANSPORT_NAME, awsSerializerInput.getTransportName());
    }

    @Test
    public void testBuilder_withSchemaNameAndTransportNameAndCompatibility_objectBuildSuccessfully() {
        AWSSerializerInput awsSerializerInput = AWSSerializerInput.builder()
                .transportName(TRANSPORT_NAME)
                .schemaName(SCHEMA_NAME)
                .compatibility(Compatibility.FORWARD_ALL)
                .build();

        assertEquals(SCHEMA_NAME, awsSerializerInput.getSchemaName());
        assertEquals(Compatibility.FORWARD_ALL, awsSerializerInput.getCompatibility());
        assertEquals(TRANSPORT_NAME, awsSerializerInput.getTransportName());
    }

    @Test
    public void testBuilder_withNullSchemaNameAndNullTransportNameAndNullCompatibility_objectBuildSuccessfully() {
        AWSSerializerInput awsSerializerInput = AWSSerializerInput.builder()
                .transportName(null)
                .schemaName(null)
                .compatibility(null)
                .build();

        assertNull(awsSerializerInput.getSchemaName());
        assertEquals(DEFAULT_TRANSPORT_NAME, awsSerializerInput.getTransportName());
        assertEquals(Compatibility.BACKWARD, awsSerializerInput.getCompatibility());
    }
}
