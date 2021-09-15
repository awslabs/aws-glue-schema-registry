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
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Data
public class AWSKafkaAvroSerializer implements Serializer<Object> {
    private static final DataFormat DATA_FORMAT = DataFormat.AVRO;
    private GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade;
    private final AwsCredentialsProvider credentialProvider;
    private final UUID schemaVersionId;
    private String schemaName;
    private AWSSchemaNamingStrategy schemaNamingStrategy;
    private boolean isKey;
    @Setter
    private String userAgentApp;

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
    public void configure(@NonNull Map<String, ?> configs,
                          boolean isKey) {
        log.info("Configuring Amazon Glue Schema Registry Service using these properties: {}", configs);
        schemaName = GlueSchemaRegistryUtils.getInstance()
                .getSchemaName(configs);
        this.isKey = isKey;

        if (schemaName == null) {
            schemaNamingStrategy = GlueSchemaRegistryUtils.getInstance()
                    .configureSchemaNamingStrategy(configs);
        }
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        if (this.userAgentApp == null) {
            //Set it to kafka if not set by upstream serializers / deserializers
            this.userAgentApp = UserAgents.KAFKA;
        }
        glueSchemaRegistryConfiguration.setUserAgentApp(this.userAgentApp);
        glueSchemaRegistrySerializationFacade = GlueSchemaRegistrySerializationFacade.builder()
            .glueSchemaRegistryConfiguration(glueSchemaRegistryConfiguration)
                .credentialProvider(credentialProvider)
                .build();
    }

    @Override
    public byte[] serialize(String topic,
                            Object data) {
        byte[] result = null;

        if (null == data) {
            return null;
        }

        UUID schemaVersionIdFromRegistry = null;
        if (this.schemaVersionId == null) {
            log.debug("Schema Version Id is null. Trying to register the schema.");
            schemaVersionIdFromRegistry =
                    glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(prepareInput(data, topic, isKey));
        } else {
            schemaVersionIdFromRegistry = this.schemaVersionId;
        }

        if (schemaVersionIdFromRegistry != null) {
            log.debug("Schema Version Id received from the from schema registry: {}", schemaVersionIdFromRegistry);
            result =
                    glueSchemaRegistrySerializationFacade.serialize(DATA_FORMAT, data, schemaVersionIdFromRegistry);
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
     * @param data data or record
     * @param isKey flag that indicates if the record is a key or not
     * @return schemaName.
     */
    private String getSchemaName(String topic,
                                 Object data,
                                 Boolean isKey) {
        if (schemaName == null) {
            return schemaNamingStrategy.getSchemaName(topic, data, isKey);
        }

        return schemaName;
    }

    private AWSSerializerInput prepareInput(@NonNull Object data,
                                            String topic,
                                            Boolean isKey) {
        return AWSSerializerInput.builder()
                .schemaDefinition(AVROUtils.getInstance()
                                          .getSchemaDefinition(data))
                .schemaName(getSchemaName(topic, data, isKey))
                .transportName(topic)
                .dataFormat(DATA_FORMAT.name())
                .build();
    }
}
