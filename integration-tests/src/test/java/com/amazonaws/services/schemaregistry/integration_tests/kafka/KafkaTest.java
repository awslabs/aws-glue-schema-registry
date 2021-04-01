package com.amazonaws.services.schemaregistry.integration_tests.kafka;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The test class for Kafka related tests for Glue Schema Registry
 */
@Slf4j
public class KafkaTest {
    private static LocalKafkaClusterHelper localKafkaClusterHelper = new LocalKafkaClusterHelper();
    private static final String TOPIC_NAME_PREFIX = "SchemaRegistryTests";
    private static final String INPUT_TOPIC_NAME_PREFIX_FOR_STREAMS = "kafkaStreamsInputTopicPrefix";
    private static final String OUTPUT_TOPIC_NAME_PREFIX_FOR_STREAMS = "kafkaStreamsOutputTopicPrefix";
    private static final String schemaRegistryEndpointOverride = Properties.ENDPOINT;
    private static final String schemaRegistryRegion = Properties.REGION;

    @Test
    public void testProduceConsume() throws Exception {
        log.info("Starting the test for producing and consuming messages via Kafka ...");

        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        final int recordsProduced = 20;
        int recordsConsumed = 0;

        kafkaHelper.doProduce(topic, recordsProduced);
        recordsConsumed = kafkaHelper.doConsume(topic);
        log.info(String.format("Producing %s records, and consuming %s records", recordsProduced, recordsConsumed));

        assertEquals(recordsConsumed, recordsProduced);
        log.info("Finish the test for producing/consuming messages via Kafka.");
    }

    @Test
    public void testProduceConsumeWithSchemaRegistry() throws Exception {
        log.info("Starting the test for producing and consuming Avro messages via Kafka ...");
        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        List<ProducerRecord<String, GenericRecord>> producerRecords = kafkaHelper.doProduceAvroRecords(topic, schemaRegistryEndpointOverride,
                schemaRegistryRegion, AWSSchemaRegistryConstants.COMPRESSION.NONE.name());
        List<ConsumerRecord<String, GenericRecord>> consumerRecords = kafkaHelper.doConsumeAvroRecords(topic,
                schemaRegistryEndpointOverride, schemaRegistryRegion);

        assertRecordsEquality(producerRecords, consumerRecords);
        log.info("Finished test for producing/consuming Avro messages via Kafka.");
    }

    @Test
    public void testProduceConsumeWithSchemaRegistryMultiThreaded() throws Exception {
        log.info("Starting the test for producing and consuming Avro messages via Kafka ...");
        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        List<ProducerRecord<String, GenericRecord>> producerRecords = kafkaHelper.doProduceAvroRecordsMultithreaded(topic, schemaRegistryEndpointOverride,
                schemaRegistryRegion,
                AWSSchemaRegistryConstants.COMPRESSION.NONE.name());
        List<ConsumerRecord<String, GenericRecord>> consumerRecords = kafkaHelper.doConsumeAvroRecords(topic, schemaRegistryEndpointOverride,
                schemaRegistryRegion);

        assertEquals(producerRecords.size(), consumerRecords.size());
        log.info("Finished test for producing/consuming Avro messages via Kafka.");
    }

    @Test
    public void testProduceConsumeWithCompressionEnabledSchemaRegistry() throws Exception {
        log.info("Starting the test for producing and consuming Avro messages via Kafka ...");
        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        List<ProducerRecord<String, GenericRecord>> producerRecords = kafkaHelper.doProduceAvroRecords(topic,
                    schemaRegistryEndpointOverride, schemaRegistryRegion, AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name());

        List<ConsumerRecord<String, GenericRecord>> consumerRecords = kafkaHelper.doConsumeAvroRecords(topic,
                schemaRegistryEndpointOverride, schemaRegistryRegion);

        assertRecordsEquality(producerRecords, consumerRecords);

        log.info("Finished test for producing/consuming Avro messages with Compression via Kafka.");
    }

    @Test
    public void testProduceConsumeWithSerDeSchemaRegistry() throws Exception {
        log.info("Serde Test Starting the test for producing and consuming Avro messages via Kafka ...");
        final Pair<String, KafkaHelper> kafkaHelperPair = createAndGetKafkaHelper(TOPIC_NAME_PREFIX);
        String topic = kafkaHelperPair.getKey();
        KafkaHelper kafkaHelper = kafkaHelperPair.getValue();

        List<ProducerRecord<String, Object>> producerRecords = kafkaHelper.doProduceAvroRecordsSerde(topic,
                schemaRegistryEndpointOverride, schemaRegistryRegion, AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name());
        List<ConsumerRecord<String, Object>> consumerRecords = kafkaHelper.doConsumeAvroRecordsSerde(topic,
                schemaRegistryEndpointOverride, schemaRegistryRegion);

        assertRecordsEquality(producerRecords, consumerRecords);

        log.info("Finish the test for producing/consuming Avro messages via Kafka with passing serde from "
                + "constructor.");
    }

    private static Pair<String, KafkaHelper> createAndGetKafkaHelper(String topicNamePrefix) throws Exception {
        final String topic = String.format("%s-%s-%s", topicNamePrefix, Instant
                .now()
                .atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yy-MM-dd-HH-mm")), RandomStringUtils.randomAlphanumeric(4));

        final String bootstrapString = localKafkaClusterHelper.getBootstrapString();
        final String zookeeperConnectString = localKafkaClusterHelper.getZookeeperConnectString();
        final KafkaHelper kafkaHelper = new KafkaHelper(bootstrapString, zookeeperConnectString, localKafkaClusterHelper.getOrCreateCluster());
        kafkaHelper.createTopic(topic, localKafkaClusterHelper.getNumberOfPartitions(), localKafkaClusterHelper.getReplicationFactor());
        return Pair.of(topic, kafkaHelper);
    }

    private <T> void assertRecordsEquality(List<ProducerRecord<String, T>> producerRecords,
                                           List<ConsumerRecord<String, T>> consumerRecords) {
        assertThat(producerRecords.size(), is(equalTo(consumerRecords.size())));
        Map<String, T> producerRecordsMap = producerRecords
                .stream()
                .collect(Collectors.toMap(ProducerRecord::key, ProducerRecord::value));

        for (ConsumerRecord<String, T> consumerRecord : consumerRecords) {
            assertThat(producerRecordsMap, hasEntry(consumerRecord.key(), consumerRecord.value()));
            assertThat(consumerRecord.value(), is(equalTo(producerRecordsMap.get(consumerRecord.key()))));
        }
    }

    private <T> void assertStreamsRecordsEquality(List<ProducerRecord<String, T>> producerRecords,
                                                  List<ConsumerRecord<String, T>> consumerRecords) {
        assertThat(producerRecords.size() - 3, is(equalTo(consumerRecords.size())));
        Map<String, T> producerRecordsMap = producerRecords
                .stream()
                .filter(record -> !"11".equals(record.key()))
                .filter(record -> !"covid-19".equals(((GenericRecord) record.value()).get("f1")))
                .filter(record -> !"morning".equals(((GenericRecord) record.value()).get("f2")))
                .collect(Collectors.toMap(ProducerRecord::key, ProducerRecord::value));

        for (ConsumerRecord<String, T> consumerRecord : consumerRecords) {
            assertThat(producerRecordsMap, hasEntry(consumerRecord.key(), consumerRecord.value()));
            assertThat(consumerRecord.value(), is(equalTo(producerRecordsMap.get(consumerRecord.key()))));
        }
    }
}
