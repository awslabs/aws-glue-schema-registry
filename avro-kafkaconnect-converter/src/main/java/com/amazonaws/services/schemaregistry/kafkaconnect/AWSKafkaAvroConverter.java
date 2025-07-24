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

package com.amazonaws.services.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroDataConfig;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Map;

/**
 * Amazon Schema Registry Avro converter for Kafka Connect users.
 */

@Slf4j
@Data
public class AWSKafkaAvroConverter implements Converter {
    private AWSKafkaAvroSerializer serializer;
    private AWSKafkaAvroDeserializer deserializer;
    private AvroData avroData;

    private boolean isKey;

    /**
     * Constructor used by Kafka Connect user.
     */
    public AWSKafkaAvroConverter() {
        serializer = new AWSKafkaAvroSerializer();
        serializer.setUserAgentApp(UserAgents.KAFKACONNECT);

        deserializer = new AWSKafkaAvroDeserializer();
        deserializer.setUserAgentApp(UserAgents.KAFKACONNECT);
    }

    public AWSKafkaAvroConverter(
            AWSKafkaAvroSerializer awsKafkaAvroSerializer,
            AWSKafkaAvroDeserializer awsKafkaAvroDeserializer,
            AvroData avroData) {
        serializer = awsKafkaAvroSerializer;
        deserializer = awsKafkaAvroDeserializer;
        this.avroData = avroData;
    }

    /**
     * Configure the AWS Avro Converter.
     * @param configs configuration elements for the converter
     * @param isKey true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        new AWSKafkaAvroConverterConfig(configs);

        //TODO: add this feature to all other converters
        String roleToAssume = (String) configs.get(AWSSchemaRegistryConstants.ASSUME_ROLE_ARN);
        if (roleToAssume != null && !roleToAssume.isEmpty()) {
            String sessionName = configs.get(AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION_NAME) != null
                    ? configs.get(AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION_NAME).toString()
                    : "kafka-connect-session";

            String region = configs.get(AWSSchemaRegistryConstants.AWS_REGION).toString();

            AwsCredentialsProvider credentialsProvider = getCredentialsProvider(roleToAssume, sessionName, region);

            deserializer = new AWSKafkaAvroDeserializer(credentialsProvider, configs);
            serializer = new AWSKafkaAvroSerializer(credentialsProvider, configs);
        }

        serializer.configure(configs, this.isKey);
        deserializer.configure(configs, this.isKey);

        avroData = new AvroData(new AvroDataConfig(configs));
    }

    /**
     * Convert orginal Connect data to AVRO serialized byte array
     * @param topic topic name
     * @param schema original Connect schema
     * @param value original Connect data
     * @return AVRO serialized byte array
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            Object avroValue = avroData.fromConnectData(schema, value);
            return serializer.serialize(topic, avroValue);
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * Convert AVRO serialized byte array to Connect schema and data
     * @param topic topic name
     * @param value AVRO serialized byte array
     * @return Connect schema and data
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        Object deserialized;

        if (value == null) {
            return SchemaAndValue.NULL;
        }

        try {
            deserialized = deserializer.deserialize(topic, value);
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema avroSchema = parser.parse(deserializer.getGlueSchemaRegistryDeserializationFacade().getSchemaDefinition(value));

        return avroData.toConnectData(avroSchema, deserialized);
    }

    @VisibleForTesting
    protected AwsCredentialsProvider getCredentialsProvider(String roleArn, String sessionName, String region) {
        UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder = UrlConnectionHttpClient.builder();
        StsClient stsClient = StsClient.builder()
                .httpClient(urlConnectionHttpClientBuilder.build())
                .region(Region.of(region))
                .build();
        return StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(assumeRoleRequest -> assumeRoleRequest
                        .roleArn(roleArn)
                        .roleSessionName(sessionName))
                .stsClient(stsClient)
                .build();
    }
}
