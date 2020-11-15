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
package com.amazonaws.services.schemaregistry.serializers;

import com.amazonaws.services.schemaregistry.common.AWSCompressionFactory;
import com.amazonaws.services.schemaregistry.common.AWSCompressionHandler;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Encodes Schema Register headers into byte buffers.
 */
public class SerializationDataEncoder {

    private final AWSCompressionHandler compressionHandler;

    private final GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs;

    public SerializationDataEncoder(GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs) {
        this.schemaRegistrySerDeConfigs = schemaRegistrySerDeConfigs;
        this.compressionHandler =
            new AWSCompressionFactory()
            .getCompressionHandler(schemaRegistrySerDeConfigs.getCompressionType());
    }

    /**
     * Schema Registry header consists of following components:
     * 1. Version byte.
     * 2. Compression byte.
     * 3. Schema Version UUID Id that represents the writer schema.
     * 4. Actual data bytes. The data can be compressed based on configuration.
     *
     * @param objectBytes bytes to add header to.
     * @return Schema Registry header encoded data.
     */
    public byte[] write(final byte[] objectBytes, UUID schemaVersionId) {
        byte[] bytes;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            writeHeaderVersionBytes(out);
            writeCompressionBytes(out);
            writeSchemaVersionId(out, schemaVersionId);

            boolean shouldCompress = this.compressionHandler != null;
            bytes = writeToExistingStream(out, shouldCompress ? compressData(objectBytes) : objectBytes);

        } catch (Exception e) {
            throw new AWSSchemaRegistryException(e.getMessage(), e);
        }

        return bytes;
    }

    private void writeCompressionBytes(ByteArrayOutputStream out) {
        out.write(compressionHandler != null ? AWSSchemaRegistryConstants.COMPRESSION_BYTE
            : AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE);
    }

    private void writeHeaderVersionBytes(ByteArrayOutputStream out) {
        out.write(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE);
    }

    private void writeSchemaVersionId(ByteArrayOutputStream out, UUID schemaVersionId) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[AWSSchemaRegistryConstants.SCHEMA_VERSION_ID_SIZE]);
        buffer.putLong(schemaVersionId.getMostSignificantBits());
        buffer.putLong(schemaVersionId.getLeastSignificantBits());
        out.write(buffer.array());
    }

    private byte[] compressData(byte[] actualDataBytes) throws IOException {
        return this.compressionHandler.compress(actualDataBytes);
    }

    private byte[] writeToExistingStream(ByteArrayOutputStream toStream, byte[] fromStream) throws IOException {
        toStream.write(fromStream);
        return toStream.toByteArray();
    }
}
