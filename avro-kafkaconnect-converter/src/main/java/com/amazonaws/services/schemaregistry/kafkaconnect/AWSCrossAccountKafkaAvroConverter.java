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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Map;

import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION;

/**
 * Amazon Schema Registry Avro converter, with Cross Account assume role, for Kafka Connect users.
 */

@Slf4j
@Data
public class AWSCrossAccountKafkaAvroConverter extends AWSKafkaAvroConverter {

    /**
     * Assume role and configure the AWS Avro Converter.
     * @param configs configuration elements for the converter
     * @param isKey true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        setKey(isKey);
        new AWSKafkaAvroConverterConfig(configs);

        String roleToAssume = (String) configs.get(AWSSchemaRegistryConstants.ASSUME_ROLE_ARN);
        if (roleToAssume == null) {
            throw new AWSSchemaRegistryException(AWSSchemaRegistryConstants.ASSUME_ROLE_ARN + " is not defined in the properties");
        }

        String sessionName = configs.get(ASSUME_ROLE_SESSION) != null
                ? configs.get(ASSUME_ROLE_SESSION).toString()
                : "kafka-connect-session";

        String region = configs.get(AWSSchemaRegistryConstants.AWS_REGION).toString();

        AwsCredentialsProvider credentialsProvider = getCredentialsProvider(roleToAssume, sessionName, region);

        setDeserializer(new AWSKafkaAvroDeserializer(credentialsProvider, configs));
        getDeserializer().setUserAgentApp(UserAgents.KAFKACONNECT);

        setSerializer(new AWSKafkaAvroSerializer(credentialsProvider, configs));
        getSerializer().setUserAgentApp(UserAgents.KAFKACONNECT);

        getSerializer().configure(configs, isKey());
        getDeserializer().configure(configs, isKey());

        setAvroData(new AvroData(new AvroDataConfig(configs)));
    }

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
