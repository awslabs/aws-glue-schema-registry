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
package com.amazonaws.services.schemaregistry.integrationtests.schemareplication;

import com.amazonaws.services.schemaregistry.integrationtests.generators.*;
import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest;
import software.amazon.awssdk.services.glue.model.SchemaId;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The test class for schema replication related tests for Glue Schema Registry
 */
@Slf4j
public class AWSGlueCrossRegionSchemaReplicationIntegrationTest {
    private static final String SRC_CLUSTER_ALIAS = "src";
    private static final String TOPIC_NAME_PREFIX = "SchemaRegistryTests";
    private static final String SCHEMA_REGISTRY_SRC_ENDPOINT_OVERRIDE = GlueSchemaRegistryConnectionProperties.SRC_ENDPOINT;
    private static final String SCHEMA_REGISTRY_DEST_ENDPOINT_OVERRIDE = GlueSchemaRegistryConnectionProperties.DEST_ENDPOINT;
    private static final String SRC_REGION = GlueSchemaRegistryConnectionProperties.SRC_REGION;
    private static final String DEST_REGION = GlueSchemaRegistryConnectionProperties.DEST_REGION;
    private static final String RECORD_TYPE = "GENERIC_RECORD";
    private static final List<Compatibility> COMPATIBILITIES = Compatibility.knownValues()
            .stream()
            .filter(c -> c.toString().equals("NONE")
                        || c.toString().equals("BACKWARD"))
            .collect(Collectors.toList());
    private static LocalKafkaClusterHelper srcKafkaClusterHelper = new LocalKafkaClusterHelper();
    private static LocalKafkaClusterHelper destKafkaClusterHelper = new LocalKafkaClusterHelper();
    private static AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder()
            .build();
    private static List<String> schemasToCleanUp = new ArrayList<>();
    private final TestDataGeneratorFactory testDataGeneratorFactory = new TestDataGeneratorFactory();

    private static Stream<Arguments> testArgumentsProvider() {
        Stream.Builder<Arguments> argumentBuilder = Stream.builder();
        for (DataFormat dataFormat : DataFormat.knownValues()) {
            for (Compatibility compatibility : COMPATIBILITIES) {
                for (AWSSchemaRegistryConstants.COMPRESSION compression :
                        AWSSchemaRegistryConstants.COMPRESSION.values()) {
                    argumentBuilder.add(Arguments.of(dataFormat, RECORD_TYPE, compatibility, compression));
                }
            }
        }
        return argumentBuilder.build();
    }

    private static Pair<String, KafkaHelper> createAndGetKafkaHelper(String topicNamePrefix) throws Exception {
        final String topic = String.format("%s-%s-%s", topicNamePrefix, Instant.now()
                .atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yy-MM-dd-HH-mm")), RandomStringUtils.randomAlphanumeric(4));

        final String srcBootstrapString = srcKafkaClusterHelper.getSrcClusterBootstrapString();
        final KafkaHelper kafkaHelper = new KafkaHelper(srcBootstrapString, srcKafkaClusterHelper.getOrCreateCluster());
        kafkaHelper.createTopic(topic, srcKafkaClusterHelper.getNumberOfPartitions(), srcKafkaClusterHelper.getReplicationFactor());
        return Pair.of(topic, kafkaHelper);
    }

    @Test
    public void testProduceConsumeWithoutGlueSchemaRegistry() throws Exception {
        log.info("Starting the test for producing and consuming messages via Kafka ...");

        final Pair<String, KafkaHelper> srcKafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = srcKafkaHelperPair.getKey();
        KafkaHelper srcKafkaHelper = srcKafkaHelperPair.getValue();
        KafkaHelper destKafkaHelper = new KafkaHelper(destKafkaClusterHelper.getDestClusterBootstrapString(), destKafkaClusterHelper.getOrCreateCluster());

        final int recordsProduced = 20;
        srcKafkaHelper.doProduce(topic, recordsProduced);

        //Delay added to allow MM2 copy the data to destination cluster
        //before consuming the records from the destination cluster
        Thread.sleep(10000);

        ConsumerProperties consumerProperties = ConsumerProperties.builder()
                .topicName(String.format("%s.%s",SRC_CLUSTER_ALIAS, topic))
                .build();

        int recordsConsumed = destKafkaHelper.doConsume(consumerProperties);
        log.info("Producing {} records, and consuming {} records", recordsProduced, recordsConsumed);

        assertEquals(recordsConsumed, recordsProduced);
        log.info("Finish the test for producing/consuming messages via Kafka.");
    }

    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    public void testProduceConsumeWithSchemaRegistryForAllThreeDataFormats(final DataFormat dataFormat,
                                                     final AvroRecordType avroRecordType,
                                                     final Compatibility compatibility) throws Exception {
        log.info("Starting the test for producing and consuming {} messages via Kafka ...", dataFormat.name());
        final Pair<String, KafkaHelper> srcKafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = srcKafkaHelperPair.getKey();
        KafkaHelper srcKafkaHelper = srcKafkaHelperPair.getValue();
        KafkaHelper destKafkaHelper = new KafkaHelper(destKafkaClusterHelper.getDestClusterBootstrapString(), destKafkaClusterHelper.getOrCreateCluster());

        TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, avroRecordType, compatibility));
        List<?> records = testDataGenerator.createRecords();

        String schemaName = String.format("%s-%s", topic, dataFormat.name());
        schemasToCleanUp.add(schemaName);

        ProducerProperties producerProperties = ProducerProperties.builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name())
                .compatibilityType(compatibility.name())
                .autoRegistrationEnabled("true")
                .build();

        List<ProducerRecord<String, Object>> producerRecords =
                srcKafkaHelper.doProduceRecords(producerProperties, records);

        //Delay added to allow MM2 copy the data to destination cluster
        //before consuming the records from the destination cluster
        Thread.sleep(10000);

        ConsumerProperties.ConsumerPropertiesBuilder consumerPropertiesBuilder = ConsumerProperties.builder()
                .topicName(String.format("%s.%s",SRC_CLUSTER_ALIAS, topic));

        consumerPropertiesBuilder.protobufMessageType(ProtobufMessageType.DYNAMIC_MESSAGE.getName());
        consumerPropertiesBuilder.avroRecordType(avroRecordType.getName()); // Only required for the case of AVRO

        List<ConsumerRecord<String, Object>> consumerRecords = destKafkaHelper.doConsumeRecords(consumerPropertiesBuilder.build());

        assertRecordsEquality(producerRecords, consumerRecords);
        log.info("Finished test for producing/consuming {} messages via Kafka.", dataFormat.name());
    }

    @AfterAll
    public static void tearDown() throws URISyntaxException {
        log.info("Starting Clean-up of schemas created with GSR.");
        GlueClient glueClientSrc = GlueClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(SRC_REGION))
                .endpointOverride(new URI(SCHEMA_REGISTRY_SRC_ENDPOINT_OVERRIDE))
                .httpClient(UrlConnectionHttpClient.builder()
                        .build())
                .build();
        GlueClient glueClientDest = GlueClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(DEST_REGION))
                .endpointOverride(new URI(SCHEMA_REGISTRY_DEST_ENDPOINT_OVERRIDE))
                .httpClient(UrlConnectionHttpClient.builder()
                        .build())
                .build();

        for (String schemaName : schemasToCleanUp) {
            log.info("Cleaning up schema {}..", schemaName);
            DeleteSchemaRequest deleteSchemaRequest = DeleteSchemaRequest.builder()
                    .schemaId(SchemaId.builder()
                            .registryName("default-registry")
                            .schemaName(schemaName)
                            .build())
                    .build();

            glueClientSrc.deleteSchema(deleteSchemaRequest);
            glueClientDest.deleteSchema(deleteSchemaRequest);
        }

        log.info("Finished Cleaning up {} schemas created with GSR.", schemasToCleanUp.size());
    }

    private <T> void assertRecordsEquality(List<ProducerRecord<String, T>> producerRecords,
                                           List<ConsumerRecord<String, T>> consumerRecords) {
        assertThat(producerRecords.size(), is(equalTo(consumerRecords.size())));
        Map<String, T> producerRecordsMap = producerRecords.stream()
                .collect(Collectors.toMap(ProducerRecord::key, ProducerRecord::value));

        for (ConsumerRecord<String, T> consumerRecord : consumerRecords) {
            assertThat(producerRecordsMap, hasKey(consumerRecord.key()));
            if (consumerRecord.value() instanceof DynamicMessage) {
                assertDynamicRecords(consumerRecord, producerRecordsMap);
            } else {
                assertThat(consumerRecord.value(), is(equalTo(producerRecordsMap.get(consumerRecord.key()))));
            }
        }
    }

    private <T> void assertDynamicRecords(ConsumerRecord<String,T> consumerRecord, Map<String,T> producerRecordsMap) {
        DynamicMessage consumerDynamicMessage = (DynamicMessage) consumerRecord.value();
        Message producerDynamicMessage = (Message) producerRecordsMap.get(consumerRecord.key());
        //In case of DynamicMessage de-serialization, we cannot equate them to POJO records,
        //so we check for their byte equality.
        assertThat(consumerDynamicMessage.toByteArray(), is(producerDynamicMessage.toByteArray()));
    }
}
