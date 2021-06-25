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

import com.amazonaws.services.schemaregistry.caching.GlueSchemaRegistryCache;
import com.amazonaws.services.schemaregistry.caching.GlueSchemaRegistrySerializerCache;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryGlueClientRetryPolicyHelper;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.cache.CacheStats;

@Slf4j
public class GlueSchemaRegistrySerializationFacade {
    private AWSSchemaRegistryClient awsSchemaRegistryClient;

    private SerializationDataEncoder serializationDataEncoder;
    private GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;

    private GlueSchemaRegistrySerializerFactory glueSchemaRegistrySerializerFactory =
            new GlueSchemaRegistrySerializerFactory();

    @Setter
    @Getter
    private GlueSchemaRegistryCache<Schema, UUID, CacheStats> cache;

    @Builder
    public GlueSchemaRegistrySerializationFacade(@NonNull AwsCredentialsProvider credentialProvider,
                                                 AWSSchemaRegistryClient schemaRegistryClient,
                                                 GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration,
                                                 Map<String, ?> configs,
                                                 Properties properties) {
        if (glueSchemaRegistryConfiguration != null) {
            this.glueSchemaRegistryConfiguration = glueSchemaRegistryConfiguration;
        } else if (configs != null) {
            this.glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        } else if (properties != null) {
            this.glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(properties);
        } else {
            throw new AWSSchemaRegistryException("Configuration map and properties cannot be null");
        }

        if (schemaRegistryClient != null) {
            this.awsSchemaRegistryClient = schemaRegistryClient;
        } else {
            this.awsSchemaRegistryClient =
                    new AWSSchemaRegistryClient(credentialProvider, this.glueSchemaRegistryConfiguration,
                                                AWSSchemaRegistryGlueClientRetryPolicyHelper.getRetryPolicy());
        }

        this.serializationDataEncoder = new SerializationDataEncoder(this.glueSchemaRegistryConfiguration);

        cache = GlueSchemaRegistrySerializerCache.getInstance(this.glueSchemaRegistryConfiguration);
    }

    public UUID getOrRegisterSchemaVersion(@NonNull AWSSerializerInput serializerInput) {
        String schemaDefinition = serializerInput.getSchemaDefinition();
        String schemaName = serializerInput.getSchemaName();
        String transportName = serializerInput.getTransportName();
        String dataFormat = serializerInput.getDataFormat();

        Schema key = new Schema(schemaDefinition, dataFormat, schemaName);
        Map<String, String> metadata = constructSchemaVersionMetadata(transportName);

        UUID schemaVersionId = cache.get(key);

        if (schemaVersionId == null) {
            schemaVersionId =
                    awsSchemaRegistryClient.getORRegisterSchemaVersionId(schemaDefinition, schemaName, dataFormat,
                                                                         metadata);
            cache.put(key, schemaVersionId);
            log.debug("Cache stats {}", cache.getCacheStats());
        }

        return schemaVersionId;
    }

    private Map<String, String> constructSchemaVersionMetadata(String transportName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, transportName);

        if (glueSchemaRegistryConfiguration.getMetadata() != null) {
            metadata.putAll(glueSchemaRegistryConfiguration.getMetadata());
        }

        return metadata;
    }

    public byte[] serialize(DataFormat dataFormat,
                            @NonNull Object data,
                            @NonNull UUID schemaVersionId) {
        GlueSchemaRegistryDataFormatSerializer dataFormatSerializer =
                glueSchemaRegistrySerializerFactory.getInstance(dataFormat, glueSchemaRegistryConfiguration);

        byte[] serializedBytes = dataFormatSerializer.serialize(data);
        return serializationDataEncoder.write(serializedBytes, schemaVersionId);
    }

    public byte[] encode(String transportName,
                         Schema schema,
                         byte[] data) {
        final String dataFormat = schema.getDataFormat();
        final String schemaDefinition = schema.getSchemaDefinition();
        final String schemaName = schema.getSchemaName();

        GlueSchemaRegistryDataFormatSerializer dataFormatSerializer =
            glueSchemaRegistrySerializerFactory.getInstance(
                DataFormat.valueOf(dataFormat), glueSchemaRegistryConfiguration);
        //Ensures the data bytes conform to schema definition for data formats like JSON.
        dataFormatSerializer.validate(schemaDefinition, data);

        UUID schemaVersionId = getOrRegisterSchemaVersion(AWSSerializerInput.builder()
                                                                  .schemaDefinition(schemaDefinition)
                                                                  .schemaName(schemaName)
                                                                  .dataFormat(dataFormat)
                                                                  .transportName(transportName)
                                                                  .build());

        return serializationDataEncoder.write(data, schemaVersionId);
    }

    public String getSchemaDefinition(DataFormat dataFormat,
                                      @NonNull Object data) {
        GlueSchemaRegistryDataFormatSerializer dataFormatSerializer =
                glueSchemaRegistrySerializerFactory.getInstance(dataFormat, glueSchemaRegistryConfiguration);

        return dataFormatSerializer.getSchemaDefinition(data);
    }
}
