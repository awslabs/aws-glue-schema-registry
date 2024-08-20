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

import com.amazonaws.services.schemaregistry.common.Schema;

/**
 * Entry point to deserialization capabilities of Glue Schema Registry client library.
 */
public interface GlueSchemaRegistryDeserializer {
    /**
     * Returns plain customer data from a Glue Schema Registry encoded Byte array.
     * All the Schema Registry specific headers, special encodings and compressions are stripped
     * matching what was sent during serialization.
     *
     * @param data byte[] Schema Registry encoded byte array.
     * @return decodedData byte[] Plain byte array with no schema registry information.
     */
    byte[] getData(byte[] data);

    /**
     * Returns the schema encoded in the byte array by Glue Schema Registry serializer.
     * The schema returned is administered by Glue Schema Registry.
     *
     * @param data byte[] Schema Registry encoded byte array.
     * @return schema {@link Schema} A Schema object representing the schema information.
     */
    Schema getSchema(byte[] data);

    /**
     * Determines if the given byte array can be deserialized by Glue Schema Registry deserializer.
     * @param data byte[] of data.
     * @return true if deserializer can decode the message, false otherwise.
     */
    boolean canDeserialize(byte[] data);

    /**
     * Overrides the UserAgentApp name attribute at runtime. This can be used to set the `app` attribute in User-Agent of the de-serializer.
     * This method overrides the User-Agent `app` attribute set using GlueSchemaRegistryConfiguration.
     * @param name AppName
     */
    void overrideUserAgentApp(String name);
}

