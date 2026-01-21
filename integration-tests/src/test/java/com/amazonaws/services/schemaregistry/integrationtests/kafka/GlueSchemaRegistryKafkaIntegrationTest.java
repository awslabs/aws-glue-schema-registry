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
package com.amazonaws.services.schemaregistry.integrationtests.kafka;

import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufClassName;
import com.amazonaws.services.schemaregistry.integrationtests.generators.AvroGenericBackwardCompatDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.JsonSchemaGenericBackwardCompatDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.ProtobufGenericBackwardDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.ProtobufSpecificNoneCompatDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorFactory;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorType;
import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The test class for Kafka related tests for Glue Schema Registry
 */
@Slf4j
public class GlueSchemaRegistryKafkaIntegrationTest {
    private static final String TOPIC_NAME_PREFIX = "SchemaRegistryTests";
    private static final String INPUT_TOPIC_NAME_PREFIX_FOR_STREAMS = "SchemaRegistryTestsStreamsInput";
    private static final String OUTPUT_TOPIC_NAME_PREFIX_FOR_STREAMS = "SchemaRegistryTestsStreamsOutput";
    private static final String SCHEMA_REGISTRY_ENDPOINT_OVERRIDE = GlueSchemaRegistryConnectionProperties.ENDPOINT;
    private static final String REGION = GlueSchemaRegistryConnectionProperties.REGION;
    private static final List<AvroRecordType> RECORD_TYPES = Arrays.stream(AvroRecordType.values())
            .filter(r -> !r.equals(AvroRecordType.UNKNOWN))
            .collect(Collectors.toList());
    private static final List<Compatibility> COMPATIBILITIES = Compatibility.knownValues()
            .stream()
            .filter(c -> c.toString()
                    .equals("NONE")) // TODO : Add Compatibility Tests for multiple compatibilities
            .collect(Collectors.toList());
    private static LocalKafkaClusterHelper localKafkaClusterHelper = new LocalKafkaClusterHelper();
    private static AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder()
            .build();
    private static List<String> schemasToCleanUp = new ArrayList<>();
    private final TestDataGeneratorFactory testDataGeneratorFactory = new TestDataGeneratorFactory();

    private static Stream<Arguments> testArgumentsProvider() {
        Stream.Builder<Arguments> argumentBuilder = Stream.builder();
        for (DataFormat dataFormat : DataFormat.knownValues()) {
            for (AvroRecordType recordType : RECORD_TYPES) {
                for (Compatibility compatibility : COMPATIBILITIES) {
                    for (AWSSchemaRegistryConstants.COMPRESSION compression :
                            AWSSchemaRegistryConstants.COMPRESSION.values()) {
                        argumentBuilder.add(Arguments.of(dataFormat, recordType, compatibility, compression));
                    }
                }
            }
        }
        return argumentBuilder.build();
    }

    @AfterAll
    public static void tearDown() throws URISyntaxException {
        log.info("Starting Clean-up of schemas created with GSR.");
        GlueClient glueClient = GlueClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(REGION))
                .endpointOverride(new URI(SCHEMA_REGISTRY_ENDPOINT_OVERRIDE))
                .build();

        for (String schemaName : schemasToCleanUp) {
            log.info("Cleaning up schema {}..", schemaName);
            DeleteSchemaRequest deleteSchemaRequest = DeleteSchemaRequest.builder()
                    .schemaId(SchemaId.builder()
                                      .registryName("default-registry")
                                      .schemaName(schemaName)
                                      .build())
                    .build();

            glueClient.deleteSchema(deleteSchemaRequest);
        }

        log.info("Finished Cleaning up {} schemas created with GSR.", schemasToCleanUp.size());
    }

    private static Pair<String, KafkaHelper> createAndGetKafkaHelper(String topicNamePrefix) throws Exception {
        final String topic = String.format("%s-%s-%s", topicNamePrefix, Instant.now()
                .atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yy-MM-dd-HH-mm")), RandomStringUtils.randomAlphanumeric(4));

        final String bootstrapString = localKafkaClusterHelper.getBootstrapString();
        final String zookeeperConnectString = localKafkaClusterHelper.getZookeeperConnectString();
        final KafkaHelper kafkaHelper =
                new KafkaHelper(bootstrapString, zookeeperConnectString, localKafkaClusterHelper.getOrCreateCluster());
        kafkaHelper.createTopic(topic, localKafkaClusterHelper.getNumberOfPartitions(),
                                localKafkaClusterHelper.getReplicationFactor());
        return Pair.of(topic, kafkaHelper);
    }

    private static Stream<Arguments> testProtobufDataProviderForPOJOs() {
        return new ProtobufSpecificNoneCompatDataGenerator().createRecords()
            .stream()
            .map(Arguments::of);
    }

    @Test
    public void testProduceConsumeWithoutGlueSchemaRegistry() throws Exception {
        log.info("Starting the test for producing and consuming messages via Kafka ...");

        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        final int recordsProduced = 20;

        kafkaHelper.doProduce(topic, recordsProduced);

        ConsumerProperties consumerProperties = ConsumerProperties.builder()
                .topicName(topic)
                .build();

        int recordsConsumed = kafkaHelper.doConsume(consumerProperties);
        log.info("Producing {} records, and consuming {} records", recordsProduced, recordsConsumed);

        assertEquals(recordsConsumed, recordsProduced);
        log.info("Finish the test for producing/consuming messages via Kafka.");
    }

    // TODO : Invalid JSON Tests
    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    public void testProduceConsumeWithSchemaRegistry(final DataFormat dataFormat,
                                                     final AvroRecordType avroRecordType,
                                                     final Compatibility compatibility,
                                                     final AWSSchemaRegistryConstants.COMPRESSION compression) throws Exception {
        log.info("Starting the test for producing and consuming {} messages via Kafka ...", dataFormat.name());
        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, avroRecordType, compatibility));
        List<?> records = testDataGenerator.createRecords();

        String schemaName = String.format("%s-%s-%s", topic, dataFormat.name(), compatibility);
        schemasToCleanUp.add(schemaName);

        ProducerProperties producerProperties = ProducerProperties.builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name())
                .compatibilityType(compatibility.name())
                .compressionType(compression.name())
                .autoRegistrationEnabled("true")
                .build();

        List<ProducerRecord<String, Object>> producerRecords =
                kafkaHelper.doProduceRecords(producerProperties, records);

        ConsumerProperties.ConsumerPropertiesBuilder consumerPropertiesBuilder = ConsumerProperties.builder().topicName(topic);
        consumerPropertiesBuilder.protobufMessageType(ProtobufMessageType.DYNAMIC_MESSAGE.getName());
        consumerPropertiesBuilder.avroRecordType(avroRecordType.getName()); // Only required for the case of AVRO

        List<ConsumerRecord<String, Object>> consumerRecords = kafkaHelper.doConsumeRecords(consumerPropertiesBuilder.build());

        assertRecordsEquality(producerRecords, consumerRecords);
        log.info("Finished test for producing/consuming {} messages via Kafka.", dataFormat.name());
    }

    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    public void testProduceConsumeWithSchemaRegistryMultiThreaded(final DataFormat dataFormat,
                                                                  final AvroRecordType avroRecordType,
                                                                  final Compatibility compatibility,
                                                                  final AWSSchemaRegistryConstants.COMPRESSION compression) throws Exception {
        log.info("Starting the test for producing and consuming {} messages via Kafka ...", dataFormat.name());
        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, avroRecordType, compatibility));
        List<?> records = testDataGenerator.createRecords();

        String schemaName = String.format("%s-%s-%s", topic, dataFormat.name(), compatibility);
        schemasToCleanUp.add(schemaName);

        ProducerProperties producerProperties = ProducerProperties.builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name())
                .compatibilityType(compatibility.name())
                .compressionType(compression.name())
                .autoRegistrationEnabled("true")
                .build();

        List<ProducerRecord<String, Object>> producerRecords =
                kafkaHelper.doProduceRecordsMultithreaded(producerProperties, records);

        ConsumerProperties.ConsumerPropertiesBuilder consumerPropertiesBuilder = ConsumerProperties.builder().topicName(topic);
        consumerPropertiesBuilder.protobufMessageType(ProtobufMessageType.DYNAMIC_MESSAGE.getName());
        consumerPropertiesBuilder.avroRecordType(avroRecordType.getName()); // Only required for the case of AVRO

        List<ConsumerRecord<String, Object>> consumerRecords = kafkaHelper.doConsumeRecords(consumerPropertiesBuilder.build());

        assertEquals(producerRecords.size(), consumerRecords.size());
        log.info("Finished test for producing/consuming {} messages via Kafka.", dataFormat.name());
    }

    @Test
    public void testProduceConsumeMultipleDataFormatRecords() throws Exception {
        AWSSchemaRegistryConstants.COMPRESSION compression = AWSSchemaRegistryConstants.COMPRESSION.ZLIB;
        Compatibility compatibility = Compatibility.NONE;
        AvroRecordType recordType = AvroRecordType.GENERIC_RECORD;

        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        List<ProducerRecord<String, Object>> producerRecords = new ArrayList<>();

        for (DataFormat dataFormat : Arrays.asList(DataFormat.AVRO, DataFormat.JSON, DataFormat.PROTOBUF)) {
            log.info("Starting the test for producing {} messages via Kafka ...", dataFormat.name());
            TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
                    TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility));
            List<?> records = Collections.singletonList(testDataGenerator.createRecords().get(0));

            String schemaName = String.format("%s-%s-%s", topic, dataFormat.name(), compatibility);
            schemasToCleanUp.add(schemaName);

            ProducerProperties producerProperties = ProducerProperties.builder()
                    .topicName(topic)
                    .schemaName(schemaName)
                    .dataFormat(dataFormat.name())
                    .compatibilityType(compatibility.name())
                    .compressionType(compression.name())
                    .autoRegistrationEnabled("true")
                    .build();

            producerRecords.addAll(kafkaHelper.doProduceRecords(producerProperties, records));
        }

        ConsumerProperties consumerProperties = ConsumerProperties.builder()
                .topicName(topic)
                .avroRecordType(recordType.getName()) // Only required for the case of AVRO
                .build();

        log.info("Starting the test for consuming multi-format messages via Kafka ...");

        List<ConsumerRecord<String, Object>> consumerRecords = kafkaHelper.doConsumeRecords(consumerProperties);

        assertEquals(producerRecords.size(), consumerRecords.size());
        log.info("Finished test for producing/consuming multi-format messages via Kafka.");
    }

    @Test
    public void testProduceConsumeWithSerDeSchemaRegistry() throws Exception {
        DataFormat dataFormat = DataFormat.AVRO;
        AWSSchemaRegistryConstants.COMPRESSION compression = AWSSchemaRegistryConstants.COMPRESSION.ZLIB;
        AvroRecordType recordType = AvroRecordType.GENERIC_RECORD;
        Compatibility compatibility = Compatibility.NONE;
        log.info("Serde Test Starting the test for producing and consuming {} messages via Kafka ...",
                 dataFormat.name());
        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility));
        List<?> records = testDataGenerator.createRecords();

        String schemaName = String.format("%s-%s-%s", topic, dataFormat.name(), compatibility);
        schemasToCleanUp.add(schemaName);

        ProducerProperties producerProperties = ProducerProperties.builder()
                .topicName(topic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name())
                .compatibilityType(compatibility.toString())
                .compressionType(compression.name())
                .autoRegistrationEnabled("true")
                .build();

        List<ProducerRecord<String, Object>> producerRecords =
                kafkaHelper.doProduceAvroRecordsSerde(producerProperties, records);

        ConsumerProperties consumerProperties = ConsumerProperties.builder()
                .topicName(topic)
                .avroRecordType(recordType.getName()) // Only required for the case of AVRO
                .build();
        List<ConsumerRecord<String, Object>> consumerRecords =
                kafkaHelper.doConsumeAvroRecordsSerde(consumerProperties);

        assertRecordsEquality(producerRecords, consumerRecords);

        log.info("Finish the test for producing/consuming {} messages via Kafka with passing serde from "
                 + "constructor.", dataFormat.name());
    }

    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testKafkaStreamsProcess(final DataFormat dataFormat) throws Exception {
        Compatibility compatibility = Compatibility.BACKWARD;
        AvroRecordType recordType = AvroRecordType.GENERIC_RECORD;
        log.info("Serde Test Starting the test for processing {} message streaming via Kafka ...", dataFormat.name());

        final Pair<String, KafkaHelper> kafkaHelperInputTopicPair =
                createAndGetKafkaHelper(INPUT_TOPIC_NAME_PREFIX_FOR_STREAMS);
        String inputTopic = kafkaHelperInputTopicPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperInputTopicPair.getValue();

        final Pair<String, KafkaHelper> kafkaHelperOutputTopicPair =
                createAndGetKafkaHelper(OUTPUT_TOPIC_NAME_PREFIX_FOR_STREAMS);
        String outputTopic = kafkaHelperOutputTopicPair.getKey();

        String schemaName = String.format("%s-%s-%s", inputTopic, dataFormat.name(), compatibility);
        schemasToCleanUp.add(schemaName);

        ProducerProperties producerProperties = ProducerProperties.builder()
                .topicName(inputTopic)
                .inputTopic(inputTopic)
                .outputTopic(outputTopic)
                .schemaName(schemaName)
                .dataFormat(dataFormat.name())
                .recordType(getRecordType(dataFormat, recordType))
                .compatibilityType(compatibility.name())
                .compressionType(AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name())
                .autoRegistrationEnabled("true")
                .build();

        TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility));
        List<?> records = testDataGenerator.createRecords();

        List<ProducerRecord<String, Object>> producerRecords =
                kafkaHelper.doProduceRecords(producerProperties, records);
        kafkaHelper.doKafkaStreamsProcess(producerProperties);

        ConsumerProperties consumerProperties = ConsumerProperties.builder()
                .topicName(outputTopic)
                .avroRecordType(recordType.getName()) // Only required for the case of AVRO
                .build();
        List<ConsumerRecord<String, Object>> consumerRecords = kafkaHelper.doConsumeRecords(consumerProperties);

        assertStreamsRecordsEquality(dataFormat, producerRecords, consumerRecords);

        log.info("Finish the test for processing {} message streaming via Kafka with passing serde from constructor.",
                 dataFormat.name());
    }


    //This test doesn't fit into the existing test framework. We have to refactor it a lot make this test case fit.
    @ParameterizedTest
    @MethodSource("testProtobufDataProviderForPOJOs")
    public void testKafkaDeserializeProtobufForPOJODeserialization(final Message message) throws Exception {
        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        //Schema name needs to be different for every test case.
        String schemaName = ProtobufClassName.normalize(message.getDescriptorForType().getFile().getFullName());
        schemasToCleanUp.add(schemaName);

        ProducerProperties producerProperties = ProducerProperties.builder()
            .topicName(topic)
            .schemaName(schemaName)
            .dataFormat(DataFormat.PROTOBUF.toString())
            .compatibilityType(Compatibility.NONE.name())
            .compressionType(AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name())
            .autoRegistrationEnabled("true")
            .build();

        List<Message> messages = Collections.singletonList(message);
        List<ProducerRecord<String, Object>> producerRecords =
            kafkaHelper.doProduceRecords(producerProperties, messages);

        ConsumerProperties consumerProperties = ConsumerProperties.builder()
            .topicName(topic)
            .protobufMessageType(ProtobufMessageType.POJO.getName())
            .build();

        List<ConsumerRecord<String, Object>> consumerRecords = kafkaHelper.doConsumeRecords(consumerProperties);

        assertRecordsEquality(producerRecords, consumerRecords);
        log.info("Finished test for producing/consuming {} POJO messages via Kafka.", DataFormat.PROTOBUF);
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

    private <T> void assertStreamsRecordsEquality(DataFormat dataFormat,
                                                  List<ProducerRecord<String, T>> producerRecords,
                                                  List<ConsumerRecord<String, T>> consumerRecords) {
        Map<String, T> producerRecordsMap;
        switch (dataFormat) {
            case AVRO:
                producerRecordsMap = producerRecords.stream()
                        .filter(producerRecord -> AvroGenericBackwardCompatDataGenerator.filterRecords(
                            (GenericRecord) producerRecord.value()))
                        .collect(Collectors.toMap(ProducerRecord::key, ProducerRecord::value));
                break;
            case JSON:
                producerRecordsMap = producerRecords.stream()
                        .filter(record -> JsonSchemaGenericBackwardCompatDataGenerator.filterRecords((JsonDataWithSchema) record.value()))
                        .collect(Collectors.toMap(ProducerRecord::key, ProducerRecord::value));
                break;
            case PROTOBUF:
                producerRecordsMap = producerRecords
                    .stream()
                    .filter(producerRecord -> ProtobufGenericBackwardDataGenerator.filterRecords(
                        (Message) producerRecord.value()))
                    .collect(Collectors.toMap(ProducerRecord::key, ProducerRecord::value));
                break;
            default:
                throw new RuntimeException("Data format is not supported");
        }

        assertThat(producerRecordsMap.size(), is(equalTo(consumerRecords.size())));
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

    private String getRecordType(DataFormat dataFormat, AvroRecordType avroRecordType) {
        if (dataFormat.equals(DataFormat.PROTOBUF)) {
            return ProtobufMessageType.DYNAMIC_MESSAGE.getName();
        }

        return avroRecordType.getName();
    }
}
