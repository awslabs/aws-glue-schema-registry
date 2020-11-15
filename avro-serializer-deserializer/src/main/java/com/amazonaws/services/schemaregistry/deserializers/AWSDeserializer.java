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

import com.amazonaws.services.schemaregistry.caching.AWSCache;
import com.amazonaws.services.schemaregistry.caching.AWSSchemaRegistryDeserializerCache;
import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSAvroDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSIncompatibleDataException;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.cache.CacheStats;

/**
 * Protocol agnostic AWS Generic de-serializer
 */
@Slf4j
public class AWSDeserializer implements Closeable {
    @Getter
    private AwsCredentialsProvider credentialsProvider;
    @Getter
    private AWSSchemaRegistryClient schemaRegistryClient;
    @Getter
    private GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;

    @Setter
    private AWSDeserializerFactory deserializerFactory;
    @Setter
    private AWSCache<UUID, Schema, CacheStats> cache;

    /**
     * Constructor accepting various dependencies.
     *
     * @param configs              configuration map
     * @param properties           configuration properties
     * @param credentialProvider   credentials provider for integrating with schema
     *                             registry service
     * @param schemaRegistryClient schema registry client for communicating with
     *                             schema registry service
     */
    @Builder
    public AWSDeserializer(Map<String, ?> configs, Properties properties, @NonNull AwsCredentialsProvider credentialProvider,
                           AWSSchemaRegistryClient schemaRegistryClient) {
        this.credentialsProvider = credentialProvider;
        if (configs != null) {
            this.glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        } else if (properties != null) {
            this.glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(properties);
        } else {
            throw new AWSSchemaRegistryException("Either properties or configuration has to be provided");
        }
        if (schemaRegistryClient != null) {
            this.schemaRegistryClient = schemaRegistryClient;
        } else {
            this.schemaRegistryClient = new AWSSchemaRegistryClient(credentialsProvider, this.glueSchemaRegistryConfiguration);
        }

        this.deserializerFactory = new AWSDeserializerFactory();
        this.cache = AWSSchemaRegistryDeserializerCache.getInstance(glueSchemaRegistryConfiguration);
    }

    public AWSDeserializer(@NonNull GlueSchemaRegistryConfiguration configuration, @NonNull AwsCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
        this.glueSchemaRegistryConfiguration = configuration;
        this.schemaRegistryClient = new AWSSchemaRegistryClient(credentialsProvider, this.glueSchemaRegistryConfiguration);
        this.deserializerFactory = new AWSDeserializerFactory();
        this.cache = AWSSchemaRegistryDeserializerCache.getInstance(glueSchemaRegistryConfiguration);
    }

    /**
     * Fetches the schema definition for the serialized data.
     *
     * @param buffer data for which schema definition is needed as ByteBuffer
     * @return schema definition
     * @throws AWSIncompatibleDataException when data is incompatible with schema
     *                                      registry
     */
    public String getSchemaDefinition(@NonNull ByteBuffer buffer) {
        AwsDeserializerSchema awsDeserializerSchema = getAwsDeserializerSchema(buffer);

        return awsDeserializerSchema.getSchema().getSchemaDefinition();
    }

    public byte[] getActualData(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        return AWSDeserializerDataParser.getInstance().getPlainData(byteBuffer);
    }

    public Schema getSchema(@NonNull byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        AwsDeserializerSchema awsDeserializerSchema = getAwsDeserializerSchema(byteBuffer);

        return awsDeserializerSchema.getSchema();
    }

    private DataFormat getDataFormat(UUID schemaVersionId) {
        DataFormat dataFormat = null;
        if (schemaVersionId != null && cache.get(schemaVersionId) != null) {
            dataFormat = DataFormat.valueOf(cache.get(schemaVersionId).getDataFormat());
        }
        return dataFormat;
    }

    /**
     * Fetches the schema definition for a the serialized data.
     *
     * @param data data for which schema definition is needed as byte array
     * @return schema for the passed data
     * @throws AWSIncompatibleDataException when data is incompatible with schema
     *                                      registry
     */
    public String getSchemaDefinition(@NonNull byte[] data) {
        return getSchemaDefinition(ByteBuffer.wrap(data));
    }

    /**
     * De-serializes the given data and returns an Object. Accepts encapsulated
     * deserializer input.
     *
     * @param deserializerInput Input data object for deserializer
     * @return de-serialized object instance
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    public Object deserialize(@NonNull AWSDeserializerInput deserializerInput) throws AWSSchemaRegistryException {
        ByteBuffer buffer = deserializerInput.getBuffer();
        AwsDeserializerSchema awsDeserializerSchema = getAwsDeserializerSchema(buffer);
        UUID schemaVersionId = awsDeserializerSchema.getSchemaVersionId();
        Schema schema = awsDeserializerSchema.getSchema();

        Object result = deserializerFactory
                .getInstance(DataFormat.valueOf(schema.getDataFormat()), this.glueSchemaRegistryConfiguration)
                .deserialize(schemaVersionId, buffer, schema.getSchemaDefinition());

        return result;
    }

    /**
     * Returns if the given data array can be deserialized.
     * @param data byte[] of data.
     * @return boolean.
     */
    public boolean canDeserialize(final byte[] data) {
        if (data == null) {
            return false;
        }
        AWSDeserializerDataParser awsDeserializerDataParser = AWSDeserializerDataParser.getInstance();
        return awsDeserializerDataParser.isDataCompatible(ByteBuffer.wrap(data), new StringBuilder());
    }

    /**
     * Get the byte array of Avro object to be de-serialized
     *
     * @param buffer byte buffer to be de-serialized
     * @return byte array of Avro object to be de-serialized
     * @throws IOException Exception during decompression
     */
    public byte[] getAvroDeserializedData(@NonNull ByteBuffer buffer) throws IOException {
        AwsDeserializerSchema awsDeserializerSchema = getAwsDeserializerSchema(buffer);
        Schema schema = awsDeserializerSchema.getSchema();
        DataFormat dataFormat = DataFormat.valueOf(schema.getDataFormat());
        AWSAvroDeserializer awsAvroDeserializer =
                (AWSAvroDeserializer) deserializerFactory.getInstance(dataFormat, this.glueSchemaRegistryConfiguration);

        return awsAvroDeserializer.getDeserializedData(buffer);
    }

    /**
     * Helper function to return schema version id and schema registry metadata
     *
     * @param buffer byte buffer to be de-serialized
     * @return schema version id and schema registry metadata
     */
    private AwsDeserializerSchema getAwsDeserializerSchema(@NonNull ByteBuffer buffer) {
        // Validate the data
        AWSDeserializerDataParser dataParser = AWSDeserializerDataParser.getInstance();

        UUID schemaVersionId = dataParser.getSchemaVersionId(buffer);
        Schema schema = retrieveSchemaRegistrySchema(schemaVersionId);

        return new AwsDeserializerSchema(schemaVersionId, schema);
    }

    /**
     * Gets the schema details for the schema version id from the schema registry.
     *
     * @param schemaVersionId the schema version Id for the writer schema
     * @return the schema for the message
     * @throws AWSSchemaRegistryException Exception when getting schema by Id from
     *                                    schema registry client
     */
    private Schema retrieveSchemaRegistrySchema(UUID schemaVersionId) throws AWSSchemaRegistryException {
        Schema schema = cache.get(schemaVersionId);

        if (schema != null) {
            return schema;
        }

        GetSchemaVersionResponse response =
                this.schemaRegistryClient.getSchemaVersionResponse(schemaVersionId.toString());

        log.debug("Retrieved writer schema from Amazon Schema Registry for message: schema version id = {}, ", schemaVersionId);

        schema = new Schema(response.schemaDefinition(), response.dataFormat()
                .name(), getSchemaName(response.schemaArn()));
        cache.put(schemaVersionId, schema);

        return schema;
    }

    private String getSchemaName(String schemaArn) {
        Arn arn = Arn.fromString(schemaArn);
        String resource = arn.resourceAsString();
        String[] splitArray = resource.split("/");
        return splitArray[splitArray.length - 1];
    }

    /**
     * Resource clean up for Closeable. This method internally shuts down the
     * background thread for publishing cloud watch metrics. After this is called, a
     * new instance of this class should be created to enable the metrics publishing
     * feature.
     */
    @Override
    public void close() {
    }

    @Data
    private static class AwsDeserializerSchema {
        private final UUID schemaVersionId;
        private final Schema schema;

        AwsDeserializerSchema(UUID schemaVersionId, Schema schema) {
            this.schemaVersionId = schemaVersionId;
            this.schema = schema;
        }
    }
}