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

import com.amazonaws.services.schemaregistry.common.Schema;

/**
 * Entry point to serialization capabilities of Glue Schema Registry client library.
 */
public interface GlueSchemaRegistrySerializer {
    /**
     * Encodes the given byte array with Schema Registry header information.
     * The header contains a reference to the Schema that corresponds to the data.
     * @param schema {@link Schema} A Schema object representing the schema information.
     * @param data Byte array consisting of customer data that needs to be encoded.
     * @return encodedData Schema Registry Encoded byte array which can only be decoded by Schema Registry de-serializer.
     */
    byte[] encode(String transportName, Schema schema, byte[] data);
}
