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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public interface AWSCompressionHandler {

    int BUFFER_SIZE = 1024;

    static byte[] writeToDeflatorObject(byte[] record, Deflater deflater) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(record.length);

        byte[] buffer = new byte[BUFFER_SIZE];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        return outputStream.toByteArray();
    }

    static Deflater getDeflatorObject(byte[] record) {
        Deflater deflater = new Deflater();
        deflater.setInput(record);
        deflater.finish();
        return deflater;
    }

    static Inflater getInflatorObject(byte[] compressedRecord, int start, int end) {
        Inflater inflater = new Inflater();
        inflater.setInput(compressedRecord, start, end);
        return inflater;
    }

    static byte[] decompress(Inflater inflater, int size) throws IOException {
        return writeToByteArrayOutputStream(inflater, new ByteArrayOutputStream(size));
    }

    static byte[] writeToByteArrayOutputStream(Inflater inflater, ByteArrayOutputStream outputStream) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        while (!inflater.finished()) {
            int count;
            try {
                count = inflater.inflate(buffer);
            } catch (DataFormatException e) {
                String message = "Bytes received is not compressed properly";
                throw new AWSSchemaRegistryException(message, e);
            }
            outputStream.write(buffer, 0, count);
        }

        outputStream.close();
        return outputStream.toByteArray();
    }

    /**
     * Compresses the record.
     *
     * @param record
     * @return compressed byte array representation.
     * @throws IOException
     */
    default byte[] compress(byte[] record) throws IOException {
        return writeToDeflatorObject(record, getDeflatorObject(record));
    }

    /**
     * Need to provide the start bit and end bit for decompressor to decompress the
     * specified bits in the byte array.
     *
     * @param compressedRecord
     * @param start
     * @param end
     * @return decompressed byte array.
     * @throws IOException
     */
    default byte[] decompress(byte[] compressedRecord, int start, int end) throws IOException {
        return decompress(getInflatorObject(compressedRecord, start, end), compressedRecord.length);
    }
}
