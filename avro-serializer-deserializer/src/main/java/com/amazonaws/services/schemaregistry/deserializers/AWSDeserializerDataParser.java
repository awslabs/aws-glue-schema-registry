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

import com.amazonaws.services.schemaregistry.common.AWSCompressionFactory;
import com.amazonaws.services.schemaregistry.exception.AWSIncompatibleDataException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Parser that understands the schema registry data format and extracts schema
 * id from serialized data, also performs data integrity validations.
 */
@Slf4j
public final class AWSDeserializerDataParser {
    private AWSCompressionFactory compressionFactory;

    /**
     * Private constructor to restrict object creation.
     */
    private AWSDeserializerDataParser(AWSCompressionFactory awsCompressionFactory) {
        this.compressionFactory = awsCompressionFactory;
    }

    /**
     * Singleton helper.
     */
    private static class DataParserHelper {
        private static final AWSDeserializerDataParser INSTANCE = new AWSDeserializerDataParser(new AWSCompressionFactory());
    }

    /**
     * Singleton instantiation helper.
     *
     * @return returns a singleton instance for AWSSchemaRegistryMetricUtils
     */
    public static AWSDeserializerDataParser getInstance() {
        return DataParserHelper.INSTANCE;
    }

    /**
     * Gets the schema version id embedded within the data.
     *
     * @param byteBuffer data from where schema version id has to be extracted as ByteBuffer
     * @return schema UUID
     * @throws AWSIncompatibleDataException when the data is incompatible with
     *                                      schema registry
     */
    public UUID getSchemaVersionId(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        // Ensure that we are not changing the buffer position.
        ByteBuffer slicedBuffer = byteBuffer.slice();

        //Make sure we have valid byteBuffer.
        validateData(slicedBuffer);

        // Skip HEADER_VERSION_BYTE
        slicedBuffer.get();
        // Skip COMPRESSION_BYTE
        slicedBuffer.get();

        long mostSigBits = slicedBuffer.getLong();
        long leastSigBits = slicedBuffer.getLong();

        return new UUID(mostSigBits, leastSigBits);
    }

    /**
     * Validates the data for compatibility with schema registry.
     *
     * @param byteBuffer   input data as byte buffer
     * @param errorBuilder error message for the validation that can be used by the
     *                     caller
     * @return true - validation success; false - otherwise
     */
    public boolean isDataCompatible(ByteBuffer byteBuffer, StringBuilder errorBuilder) {
        // Ensure that we are not changing the buffer position.
        byteBuffer.rewind();
        ByteBuffer toValidate = byteBuffer.slice();

        // We should be at least 18 bytes long
        if (toValidate.limit() < 18) {
            String message = String.format("%s size: %d", AWSIncompatibleDataException.UNKNOWN_DATA_ERROR_MESSAGE,
                toValidate.limit());
            errorBuilder.append(message);
            log.debug(message);
            return false;
        }

        Byte headerVersionByte = toValidate.get();
        if (!headerVersionByte.equals(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE)) {
            String message = AWSIncompatibleDataException.UNKNOWN_HEADER_VERSION_BYTE_ERROR_MESSAGE;
            errorBuilder.append(message);
            log.debug(message);
            return false;
        }

        Byte compressionByte = toValidate.get();
        if (!compressionByte.equals(AWSSchemaRegistryConstants.COMPRESSION_BYTE)
            && !compressionByte.equals(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE)) {
            String message = AWSIncompatibleDataException.UNKNOWN_COMPRESSION_BYTE_ERROR_MESSAGE;
            errorBuilder.append(message);
            log.debug(message);
            return false;
        }

        return true;
    }

    public byte[] getPlainData(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        ByteBuffer slicedBuffer = byteBuffer.slice();

        //Make sure we have the right bytebuffer.
        validateData(slicedBuffer);

        //Seek header byte
        slicedBuffer.get();

        //Seek compression byte.
        Byte compressionByte = slicedBuffer.get();

        //Seek SchemaVersionId bytes
        //Most significant
        slicedBuffer.getLong();
        //Least significant
        slicedBuffer.getLong();

        //Get the actual data.
        byte[] plainData = new byte[slicedBuffer.remaining()];
        slicedBuffer.get(plainData);

        boolean isCompressionEnabled = isCompressionByteSet(compressionByte);

        if (!isCompressionEnabled) {
            return plainData;
        }

        //Decompress the data and return.
        int dataStart = getSchemaRegistryHeaderLength();
        int dataEnd = slicedBuffer.limit() - dataStart;
        return decompressData(compressionByte, slicedBuffer, dataStart, dataEnd);
    }

    @SneakyThrows
    private byte[] decompressData(Byte compressionByte, ByteBuffer compressedData, int start, int end) {
        return compressionFactory
                .getCompressionHandler(compressionByte)
                .decompress(compressedData.array(), start, end);
    }

    /**
     * Helper method for validating the data.
     *
     * @param buffer     data to be de-serialized as ByteBuffer
     */
    private void validateData(@NonNull ByteBuffer buffer) throws AWSIncompatibleDataException {
        StringBuilder errorMessageBuilder = new StringBuilder();
        if (!isDataCompatible(buffer, errorMessageBuilder)) {
            throw new AWSIncompatibleDataException(errorMessageBuilder.toString());
        }
    }

    /**
     * Is Compression enabled
     *
     * @param byteBuffer byte buffer
     * @return whether the byte buffer has been compressed
     */
    public boolean isCompressionEnabled(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        ByteBuffer slicedBuffer = byteBuffer.slice();

        // skip the first byte.
        slicedBuffer.get();
        Byte compressionByte = slicedBuffer.get();
        return isCompressionByteSet(compressionByte);
    }

    private boolean isCompressionByteSet(Byte compressionByte) {
        return !compressionByte.equals(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE);
    }

    /**
     * Get the compression byte.
     *
     * @param byteBuffer byte buffer
     * @return compression byte
     */
    public Byte getCompressionByte(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        ByteBuffer slicedBuffer = byteBuffer.slice();
        // skip the first byte.
        slicedBuffer.get();
        return slicedBuffer.get();
    }

    /**
     * Get the header version byte.
     *
     * @param byteBuffer byte buffer
     * @return header byte
     */
    public Byte getHeaderVersionByte(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        ByteBuffer slicedBuffer = byteBuffer.slice();

        return slicedBuffer.get();
    }

    private int getSchemaRegistryHeaderLength() {
        return AWSSchemaRegistryConstants.HEADER_VERSION_BYTE_SIZE
                        + AWSSchemaRegistryConstants.COMPRESSION_BYTE_SIZE
                        + AWSSchemaRegistryConstants.SCHEMA_VERSION_ID_SIZE;
    }
}
