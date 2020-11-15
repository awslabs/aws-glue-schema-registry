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

package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;

import lombok.extern.slf4j.Slf4j;

/**
 * Compresses and Decompresses records using the Zlib algorithm.
 */
@Slf4j
public class AWSSchemaRegistryDefaultCompression implements AWSCompressionHandler {

    private static final String KILO_BYTES = "KB";

    @Override
    public byte[] compress(byte[] record) {
        byte[] compressed = null;
        try {
            compressed = AWSCompressionHandler.super.compress(record);

            log.debug("Compression :: record length: {}", formatDataLengthInKB(record.length));
            log.debug("Compression :: record length after compression: {}", formatDataLengthInKB(compressed.length));
        } catch (Exception e) {
            String message = "Error while compressing data";
            log.error(message, e);
            throw new AWSSchemaRegistryException(message, e);
        }

        return compressed;
    }

    @Override
    public byte[] decompress(byte[] compressedRecord, int start, int end) {
        byte[] deCompressedRecord = null;

        try {
            deCompressedRecord = AWSCompressionHandler.super.decompress(compressedRecord, start, end);
            log.debug("Decompression :: Compressed record length: {}", formatDataLengthInKB(compressedRecord.length));
            log.debug("Decompression :: Decompressed record length: {}",
                    formatDataLengthInKB(deCompressedRecord.length));
        } catch (Exception e) {
            String message = "Error while decompressing data";
            log.error(message, e);
            throw new AWSSchemaRegistryException(message, e);
        }

        return deCompressedRecord;
    }

    private String formatDataLengthInKB(int dataLength) {
        return (dataLength / 1024) + KILO_BYTES;
    }

}
