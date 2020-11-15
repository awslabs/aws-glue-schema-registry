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

import com.amazonaws.services.schemaregistry.caching.AWSCache;
import com.amazonaws.services.schemaregistry.caching.AWSSchemaRegistrySerializerCache;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryGlueClientRetryPolicyHelper;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.avro.AvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.cache.CacheStats;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class GlueSchemaRegistrySerializationFacade {
    private AWSSchemaRegistryClient awsSchemaRegistryClient;
    private static final String AVRO_SCHEMA_TYPE = DataFormat.AVRO.name();

    private SerializationDataEncoder serializationDataEncoder;
    private GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;

    @Setter
    @Getter
    private AWSCache<Schema, UUID, CacheStats> cache;

    @Builder
    public GlueSchemaRegistrySerializationFacade(@NonNull AwsCredentialsProvider credentialProvider,
        AWSSchemaRegistryClient schemaRegistryClient, @NonNull GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {

        this.glueSchemaRegistryConfiguration = glueSchemaRegistryConfiguration;

        if (schemaRegistryClient != null) {
            this.awsSchemaRegistryClient = schemaRegistryClient;
        } else {
            this.awsSchemaRegistryClient = new AWSSchemaRegistryClient(credentialProvider,
                                                                       glueSchemaRegistryConfiguration,
                                                                       AWSSchemaRegistryGlueClientRetryPolicyHelper.getRetryPolicy());
        }

        this.serializationDataEncoder = new SerializationDataEncoder(glueSchemaRegistryConfiguration);

        cache = AWSSchemaRegistrySerializerCache.getInstance(glueSchemaRegistryConfiguration);
    }

    public UUID getOrRegisterSchemaVersion(@NonNull AWSSerializerInput serializerInput) {
        String schemaDefinition = serializerInput.getSchemaDefinition();
        String schemaName = serializerInput.getSchemaName();
        String transportName = serializerInput.getTransportName();

        Schema key = new Schema(schemaDefinition, AVRO_SCHEMA_TYPE, schemaName);
        Map<String, String> metadata = constructSchemaVersionMetadata(transportName);

        UUID schemaVersionId = cache.get(key);

        if (schemaVersionId == null) {
            schemaVersionId = awsSchemaRegistryClient
                .getORRegisterSchemaVersionId(schemaDefinition, schemaName, AVRO_SCHEMA_TYPE, metadata);
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

    public byte[] serialize(DataFormat dataFormat, @NonNull Object data,
        @NonNull UUID schemaVersionId) {
        if (!DataFormat.AVRO.equals(dataFormat)) {
            throw new AWSSchemaRegistryException("Unsupported data format: " + dataFormat);
        }
        byte[] avroSerializedBytes =
            new AvroSerializer(AVROUtils.getInstance().getSchema(data)).serialize(data);

        return serializationDataEncoder.write(avroSerializedBytes, schemaVersionId);
    }

    public byte[] encode(String transportName, Schema schema, byte[] data) {
        UUID schemaVersionId = getOrRegisterSchemaVersion(
            AWSSerializerInput
                .builder()
                .schemaDefinition(schema.getSchemaDefinition())
                .schemaName(schema.getSchemaName())
                .transportName(transportName)
                .build()
        );

        return serializationDataEncoder.write(data, schemaVersionId);
    }
}
