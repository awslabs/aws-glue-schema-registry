package com.amazonaws.services.schemaregistry.common;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AWSDeserializerInputTest {
    private final String TRANSPORT_NAME = "test-transport-name";
    private final String DEFAULT_TRANSPORT_NAME = "default-stream";

    @Test
    public void testBuilder_withByteBufferAndTransportName_objectBuildSuccessfully() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        AWSDeserializerInput awsDeserializerInput = AWSDeserializerInput.builder()
                .buffer(byteBuffer)
                .transportName(TRANSPORT_NAME)
                .build();

        assertEquals(TRANSPORT_NAME, awsDeserializerInput.getTransportName());
        assertEquals(byteBuffer, awsDeserializerInput.getBuffer());
    }

    @Test
    public void testBuilder_withByteBufferAndNullTransportName_objectBuildSuccessfully() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        AWSDeserializerInput awsDeserializerInput = AWSDeserializerInput.builder()
                .buffer(byteBuffer)
                .transportName(null)
                .build();

        assertEquals(DEFAULT_TRANSPORT_NAME, awsDeserializerInput.getTransportName());
        assertEquals(byteBuffer, awsDeserializerInput.getBuffer());
    }

    @Test
    public void testBuilder_nullByteBuffer_throwsException() {
        assertThrows(IllegalArgumentException.class , () ->  AWSDeserializerInput.builder()
                .buffer(null)
                .transportName(TRANSPORT_NAME)
                .build());
    }
}
