/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.schemaregistry.deserializers;

import com.amazonaws.services.schemaregistry.exception.AWSIncompatibleDataException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AWSDeserializerDataParserTest {
    /**
     * Helper method to construct a serialized message from the supplied byte parameters with UUID.
     *
     * @param headerVersionByte header version byte for schema registry
     * @param compressionByte   compression byte for schema registry
     * @param uuid              schema version id
     * @return constructed byte array of the message
     */
    private byte[] constructSerializedData(byte headerVersionByte, byte compressionByte, UUID uuid) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[18]);

        byteBuffer.put(headerVersionByte);
        byteBuffer.put(compressionByte);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        return byteBuffer.array();
    }

    /**
     * Tests the isDataCompatible for failure case where the compression byte is unknown.
     */
    @Test
    public void test_InvalidHeader_ThrowsAWSIncompatibleDataException() {
        byte[] serializedData = constructSerializedData((byte) 99, AWSSchemaRegistryConstants.COMPRESSION_BYTE,
                UUID.randomUUID());
        Exception exception = assertThrows(AWSIncompatibleDataException.class,
                () -> AWSDeserializerDataParser
                .getInstance().getSchemaVersionId(ByteBuffer.wrap(serializedData)));
        assertEquals(AWSIncompatibleDataException.UNKNOWN_HEADER_VERSION_BYTE_ERROR_MESSAGE, exception.getMessage());
    }

    /**
     * Tests the isDataCompatible for failure case where the header version byte is unknown.
     */
    @Test
    public void test_Invalid_Compression_Byte() {
        byte[] serializedData = constructSerializedData(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, (byte) 99,
                UUID.randomUUID());
        StringBuilder errorBuilder = new StringBuilder();
        assertFalse(AWSDeserializerDataParser.getInstance().isDataCompatible(ByteBuffer.wrap(serializedData),
                errorBuilder));
        assertEquals(AWSIncompatibleDataException.UNKNOWN_COMPRESSION_BYTE_ERROR_MESSAGE, errorBuilder.toString());
    }

    /**
     * Tests the when the buffer length is invalid.
     */
    @Test
    public void test_Invalid_Length() {
        StringBuilder errorBuilder = new StringBuilder();
        assertFalse(AWSDeserializerDataParser.getInstance().isDataCompatible(ByteBuffer.wrap(new byte[2]),
                errorBuilder));
        assertTrue(errorBuilder.toString().contains(AWSIncompatibleDataException.UNKNOWN_DATA_ERROR_MESSAGE));
    }

    /**
     * Tests the isDataCompatible for success case where the header version byte is unknown.
     */
    @Test
    public void test_Success() {
        StringBuilder errorBuilder = new StringBuilder();
        byte[] serializedData = constructSerializedData(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE, UUID.randomUUID());
        assertTrue(AWSDeserializerDataParser.getInstance().isDataCompatible(ByteBuffer.wrap(serializedData),
                errorBuilder));
    }
}
