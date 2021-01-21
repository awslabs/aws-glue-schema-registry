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

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryUtils;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Data
public class AWSKafkaAvroSerializer implements Serializer<Object> {
    private AWSAvroSerializer avroSerializer;
    private final AwsCredentialsProvider credentialProvider;
    private final UUID schemaVersionId;
    private String schemaName;
    private AWSSchemaNamingStrategy schemaNamingStrategy;

    /**
     * Constructor used by Kafka producer when passing as the property.
     */
    public AWSKafkaAvroSerializer() {
        this(DefaultCredentialsProvider.builder().build(), null, null);
    }

    public AWSKafkaAvroSerializer(Map<String, ?> configs) {
        this(DefaultCredentialsProvider.builder().build(), null, configs);
    }

    public AWSKafkaAvroSerializer(AwsCredentialsProvider credentialProvider, Map<String, ?> configs) {
        this(credentialProvider, null, configs);
    }

    public AWSKafkaAvroSerializer(@NonNull Map<String, ?> configs, UUID schemaVersionId) {
        this(DefaultCredentialsProvider.builder().build(), schemaVersionId, configs);
    }

    public AWSKafkaAvroSerializer(AwsCredentialsProvider credentialProvider, UUID schemaVersionId, Map<String, ?> configs) {
        this.credentialProvider = (credentialProvider == null)
                                  ? DefaultCredentialsProvider.builder().build()
                                  : credentialProvider;
        this.schemaVersionId = schemaVersionId;
        if (configs != null) {
            configure(configs, false);
        }
    }

    @Override
    public void configure(@NonNull Map<String, ?> configs, boolean isKey) {
        log.info("Configuring Amazon Glue Schema Registry Service using these properties: {}", configs);
        schemaName = AWSSchemaRegistryUtils.getInstance().getSchemaName(configs);

        if (schemaName == null) {
            schemaNamingStrategy = AWSSchemaRegistryUtils.getInstance().configureSchemaNamingStrategy(configs);
        }

        avroSerializer = AWSAvroSerializer.builder().configs(configs).credentialProvider(credentialProvider).build();
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        byte[] result = null;

        if (null == data) {
            return null;
        }

        UUID schemaVersionIdFromRegistry = null;
        if (this.schemaVersionId == null) {
            log.debug("Schema Version Id is null. Trying to register the schema.");
            schemaVersionIdFromRegistry = avroSerializer.registerSchema(prepareInput(data, topic));
        } else {
            schemaVersionIdFromRegistry = this.schemaVersionId;
        }

        if (schemaVersionIdFromRegistry != null) {
            log.debug("Schema Version Id received from schema registry: {}", schemaVersionIdFromRegistry);
            result = avroSerializer.serialize(data, schemaVersionIdFromRegistry);
        }

        return result;
    }

    @Override
    public void close() { }

    /**
     * Provide implementation of AWSSchemaNamingStrategy via a dynamic configuration.
     * OR
     * Provide the schema name in the dynamic configuration.
     * OR
     * Client will generate the one using the AWSSchemaNamingStrategyDefaultImpl
     *
     * @param topic Name of the topic
     * @return schemaName.
     */
    private String getSchemaName(String topic, Object data) {
        if (schemaName == null) {
            return schemaNamingStrategy.getSchemaName(topic, data);
        }

        return schemaName;
    }

    private AWSSerializerInput prepareInput(@NonNull Object data,
                                            String topic) {
        return AWSSerializerInput.builder()
                .schemaDefinition(AVROUtils.getInstance()
                                          .getSchemaDefinition(data))
                .schemaName(getSchemaName(topic, data))
                .transportName(topic)
                .build();
    }
}
