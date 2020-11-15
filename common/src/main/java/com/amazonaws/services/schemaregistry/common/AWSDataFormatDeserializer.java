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

import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Interface for all schemaType/protocol/dataformat specific de-serializer implementations.
 */
public interface AWSDataFormatDeserializer {
    /**
     * De-serializes the given byte array to an Object.
     *
     * @param data   data to de-serialize as byte array
     * @param schema schema for the data
     * @return de-serialized object
     */
    Object deserialize(@NonNull byte[] data, @NonNull String schema);

    /**
     * De-serializes the given ByteBuffer to an Object. Overload without accepting schema version id.
     *
     * @param buffer data to de-serialize as ByteBuffer
     * @param schema schema for the data
     * @return de-serialized object
     */
    Object deserialize(@NonNull ByteBuffer buffer, @NonNull String schema);

    /**
     * De-serializes the given ByteBuffer to an Object.
     *
     * @param schemaVersionId schema version id
     * @param data     data to de-serialize as byte array
     * @param schema   schema for the data
     * @return de-serialized object
     */
    Object deserialize(@NonNull UUID schemaVersionId, @NonNull ByteBuffer data, @NonNull String schema);
}
