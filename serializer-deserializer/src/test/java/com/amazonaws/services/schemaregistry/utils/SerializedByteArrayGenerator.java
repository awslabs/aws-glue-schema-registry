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
package com.amazonaws.services.schemaregistry.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class SerializedByteArrayGenerator {

    /**
     * Helper method to construct a serialized message from the supplied byte parameters with UUID.
     *
     * @param headerVersionByte header version byte for schema registry
     * @param compressionByte   compression byte for schema registry
     * @param uuid              schema version id
     * @return constructed byte array of the message
     */
    public static byte[] constructBasicSerializedData(byte headerVersionByte, byte compressionByte, UUID uuid) {
        ByteBuffer byteBuffer = constructBasicSerializedByteBuffer(headerVersionByte, compressionByte, uuid);
        return byteBuffer.array();
    }

    /**
     * Helper method to construct a serialized message from the supplied byte parameters with UUID.
     *
     * @param headerVersionByte header version byte for schema registry
     * @param compressionByte   compression byte for schema registry
     * @param uuid              schema version id
     * @return constructed byte buffer of the message
     */
    public static ByteBuffer constructBasicSerializedByteBuffer(byte headerVersionByte, byte compressionByte,
                                                                UUID uuid) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[18]);

        byteBuffer.put(headerVersionByte);
        byteBuffer.put(compressionByte);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        return byteBuffer;
    }
}
