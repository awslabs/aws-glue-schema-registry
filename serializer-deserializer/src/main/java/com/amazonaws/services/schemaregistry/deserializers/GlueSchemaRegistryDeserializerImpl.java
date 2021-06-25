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
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * {@inheritDoc}
 */
public class GlueSchemaRegistryDeserializerImpl implements GlueSchemaRegistryDeserializer {
    private GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade;

    /**
     * Initialize an instance of GlueSchemaRegistryDeserializer with Properties.
     * See documentation for supported configuration property format.
     * @param awsCredentialsProvider {@link AwsCredentialsProvider}
     * @param configuration {@link GlueSchemaRegistryConfiguration} Schema Registry configuration.
     */
    public GlueSchemaRegistryDeserializerImpl(
            final AwsCredentialsProvider awsCredentialsProvider,
            final GlueSchemaRegistryConfiguration configuration) {
        this.glueSchemaRegistryDeserializationFacade = new GlueSchemaRegistryDeserializationFacade(configuration, awsCredentialsProvider);
    }

    @VisibleForTesting
    protected GlueSchemaRegistryDeserializerImpl(final GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade) {
        this.glueSchemaRegistryDeserializationFacade = glueSchemaRegistryDeserializationFacade;
    }

    /**
     * {@inheritDoc}
     * @param encodedData Schema Registry encoded data.
     * @return decodedData byte[] Plain byte array with no schema registry information.
     */
    @Override
    public byte[] getData(final byte[] encodedData) {
        return glueSchemaRegistryDeserializationFacade.getActualData(encodedData);
    }

    /**
     * {@inheritDoc}
     * @param data byte[] Schema Registry encoded byte array.
     * @return schema {@link Schema} A Schema object representing the schema information.
     */
    @Override
    public Schema getSchema(final byte[] data) {
        return glueSchemaRegistryDeserializationFacade.getSchema(data);
    }

    /**
     * {@inheritDoc}
     * @param data byte[] of data.
     * @return true if data can be decoded, false otherwise.
     */
    @Override
    public boolean canDeserialize(final byte[] data) {
        return glueSchemaRegistryDeserializationFacade.canDeserialize(data);
    }
}
