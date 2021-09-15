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
package com.amazonaws.services.schemaregistry.deserializers.avro;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.deserializers.SecondaryDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.nio.ByteBuffer;
import java.util.Map;


/**
 * AWS Kafka Avro Deserializer responsible for de-serializing the data using
 * Avro protocol serializer.
 */
@Slf4j
public class AWSKafkaAvroDeserializer implements Deserializer<Object> {
    @Getter
    private final AwsCredentialsProvider credentialProvider;
    @Getter
    @Setter
    private GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade;

    @Setter
    private String userAgentApp;

    private SecondaryDeserializer secondaryDeserializer = SecondaryDeserializer.newInstance();

    /**
     * Constructor used by Kafka consumer.
     */
    public AWSKafkaAvroDeserializer() {
        this(DefaultCredentialsProvider.builder().build(), null);
    }

    public AWSKafkaAvroDeserializer(@NonNull Map<String, ?> configs) {
        this(DefaultCredentialsProvider.builder().build(), configs);
    }

    /**
     * Constructor accepting AWSCredentialsProvider.
     *
     * @param credentialProvider AWSCredentialsProvider instance.
     */
    public AWSKafkaAvroDeserializer(AwsCredentialsProvider credentialProvider, Map<String, ?> configs) {
        this.credentialProvider = credentialProvider;
        if (configs != null) {
            configure(configs, false);
        }
    }

    /**
     * Configuration method for injecting configuration properties.
     *
     * @param configs configuration elements for de-serializer
     * @param isKey   true if key, false otherwise
     */
    @Override
    public void configure(@NonNull Map<String, ?> configs, boolean isKey) {
        log.info("Configuring Amazon Glue Schema Registry Service using these properties: {}", configs.toString());
        if (this.userAgentApp == null) {
            this.userAgentApp = UserAgents.KAFKA;
        }
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        glueSchemaRegistryConfiguration.setUserAgentApp(this.userAgentApp);
        this.glueSchemaRegistryDeserializationFacade =
            new GlueSchemaRegistryDeserializationFacade(glueSchemaRegistryConfiguration, this.credentialProvider);

        if (configs.containsKey(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER)) {
            configureSecondaryDeser(configs, isKey);
        }
    }

    /**
     * De-serialize operation for de-serializing the byte array to an Object.
     *
     * @param topic Kafka topic name
     * @param data  serialized data to be de-serialized in byte array
     * @return de-serialized object instance
     */
    @Override
    public Object deserialize(String topic, byte[] data) {
        Object result;

        if (data == null) {
            return null;
        }

        Byte headerVersionByte = getHeaderVersionByte(data);
        result = deserializeByHeaderVersionByte(topic, data, headerVersionByte);

        return result;
    }

    /**
     * Resource clean up for Closeable. This method internally shuts down the
     * background thread for publishing cloud watch metrics. After this is called, a
     * new instance of this class should be created to enable the metrics publishing
     * feature.
     */
    @Override
    public void close() {
        this.glueSchemaRegistryDeserializationFacade.close();
    }

    private AWSDeserializerInput prepareInput(byte[] data, String topic) {
        return AWSDeserializerInput.builder().buffer(ByteBuffer.wrap(data)).transportName(topic).build();
    }

    /**
     * Configure the secondary de-serializer and validate if it's from Kafka.
     */
    private void configureSecondaryDeser(Map<String, ?> configs, boolean isKey) {
        if (!secondaryDeserializer.validate(configs)) {
            throw new AWSSchemaRegistryException("The secondary deserializer is not from Kafka");
        }
        secondaryDeserializer.configure(configs, isKey);
    }

    /**
     * De-serialize operation depend on the value of header version byte.
     */
    private Object deserializeByHeaderVersionByte(String topic, byte[] data, Byte headerVersionByte) {
        return headerVersionByte.equals(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE)
                ? this.glueSchemaRegistryDeserializationFacade.deserialize(prepareInput(data, topic))
                : secondaryDeserializer.deserialize(topic, data);
    }

    private Byte getHeaderVersionByte(byte[] data) {
        GlueSchemaRegistryDeserializerDataParser dataParser = GlueSchemaRegistryDeserializerDataParser.getInstance();
        return dataParser.getHeaderVersionByte(ByteBuffer.wrap(data));
    }

}

