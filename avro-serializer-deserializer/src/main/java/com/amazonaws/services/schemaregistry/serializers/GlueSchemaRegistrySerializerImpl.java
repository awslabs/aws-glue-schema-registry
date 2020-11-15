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
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import javax.annotation.Nullable;

/**
 * {@inheritDoc}
 */
public class GlueSchemaRegistrySerializerImpl implements GlueSchemaRegistrySerializer {
    private final GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade;

    /**
     * Initialize an instance of GlueSchemaRegistrySerializer with Region.
     * @param awsCredentialsProvider {@link AwsCredentialsProvider}
     */
    public GlueSchemaRegistrySerializerImpl(
        final AwsCredentialsProvider awsCredentialsProvider,
        final String region) {
        this.glueSchemaRegistrySerializationFacade = new GlueSchemaRegistrySerializationFacade(
            awsCredentialsProvider,
            null,
            new GlueSchemaRegistryConfiguration(region)
        );
    }

    /**
     * Initialize an instance of GlueSchemaRegistrySerializer with .
     * See documentation for supported configuration entries.
     * @param awsCredentialsProvider {@link AwsCredentialsProvider}
     * @param glueSchemaRegistryConfiguration {@link GlueSchemaRegistryConfiguration} Configuration object.
     */
    public GlueSchemaRegistrySerializerImpl(
        final AwsCredentialsProvider awsCredentialsProvider,
        final GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {
        this.glueSchemaRegistrySerializationFacade = new GlueSchemaRegistrySerializationFacade(
            awsCredentialsProvider,
            null,
            glueSchemaRegistryConfiguration
        );
    }

    @VisibleForTesting
    protected GlueSchemaRegistrySerializerImpl(
        final GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade) {
        this.glueSchemaRegistrySerializationFacade = glueSchemaRegistrySerializationFacade;
    }

    /**
     * Converts the given data byte array to be Glue Schema Registry compatible byte array.
     * If the auto-registration setting is turned on, a new schema definitions will be automatically registered.
     * Note that the encoded byte array can only be decoded by a
     * Glue Schema Registry de-serializer ({@link com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer}
     *
     * @param transportName {@link String} Name of the transport channel for the message.
     *                                    This will be used to add metadata to schema.
     *                                    If null, "default-stream" will be used.
     * @param schema {@link Schema} A schema object.
     * @param data byte array of data that needs to be encoded.
     * @return Encoded Glue Schema Registry compatible byte array.
     */
    @Override
    public byte[] encode(@Nullable String transportName, Schema schema, byte[] data) {
        return glueSchemaRegistrySerializationFacade.encode(
            transportName,
            schema,
            data
        );
    }
}
