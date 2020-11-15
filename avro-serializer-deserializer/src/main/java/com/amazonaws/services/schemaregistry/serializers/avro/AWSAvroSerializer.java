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

package com.amazonaws.services.schemaregistry.serializers.avro;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Amazon Schema Registry Avro serializer.
 */

@Slf4j
public class AWSAvroSerializer {
    private GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade;

    @Builder
    public AWSAvroSerializer(@NonNull AwsCredentialsProvider credentialProvider,
            AWSSchemaRegistryClient schemaRegistryClient, Map<String, ?> configs, Properties properties) {

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;
        if (configs != null) {
            glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        } else if (properties != null) {
            glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(properties);
        } else {
            throw new AWSSchemaRegistryException("Configuration map and properties cannot be null");
        }

        this.glueSchemaRegistrySerializationFacade = new GlueSchemaRegistrySerializationFacade(
            credentialProvider, schemaRegistryClient, glueSchemaRegistryConfiguration);
    }

    public UUID registerSchema(@NonNull AWSSerializerInput serializerInput) {
       return glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(serializerInput);
    }

    public byte[] serialize(@NonNull Object data, @NonNull UUID schemaVersionId) {
        return glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, data, schemaVersionId);
    }
}
