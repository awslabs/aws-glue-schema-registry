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

/**
 * Interface for all schemaType/protocol/dataformat specific serializer implementations.
 */
public interface GlueSchemaRegistryDataFormatSerializer {
    /**
     * serializes the given Object to an byte array.
     *
     * @param data data to de-serialize as byte array
     * @return serialized byte array
     */
    byte[] serialize(@NonNull Object data);

    /**
     * Gets schema definition from Object
     *
     * @param object
     * @return
     */
    String getSchemaDefinition(@NonNull Object object);

    /**
     * Validate the given data against the schema definition if the implementing format supports it.
     *
     * @param schemaDefinition SchemaDefinition as String.
     * @param data Data as byte array.
     */
    void validate(@NonNull String schemaDefinition, @NonNull byte[] data);

    /**
     * Validate the data format object to ensure it conforms to schema if implementation supports it.
     * @param data DataFormat specific object.
     */
    void validate(@NonNull Object data);
}