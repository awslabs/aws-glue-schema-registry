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

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.integrationtests.generators.AvroGenericBackwardCompatDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.JsonSchemaGenericBackwardCompatDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.ProtobufGenericBackwardDataGenerator;
import com.amazonaws.services.schemaregistry.kafkastreams.GlueSchemaRegistryKafkaStreamsSerde;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

@Slf4j
public class KafkaHelper {
    private static final Duration CONSUMER_RUNTIME = Duration.ofMillis(10000);
    private final String bootstrapBrokers;
    private final String clusterArn;
    private final String zookeeperConnect;

    public KafkaHelper(final String bootstrapString, final String zookeeperConnectString, final String clusterArn) {
        this.bootstrapBrokers = bootstrapString;
        this.zookeeperConnect = zookeeperConnectString;
        this.clusterArn = clusterArn;
    }

    /**
     * Helper function to create test topic
     *
     * @param topic             topic name to be created
     * @param numPartitions     number of numPartitions
     * @param replicationFactor replicationFactor count
     * @throws Exception
     */
    public void createTopic(final String topic, final int numPartitions, final short replicationFactor) throws Exception {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapBrokers);
        properties.put("client.id", "gsr-integration-tests");

        log.info("Creating Kafka topic {} with bootstrap {}...", topic, bootstrapBrokers);
        try (AdminClient kafkaAdminClient = AdminClient.create(properties)) {
            final NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
            final CreateTopicsResult createTopicsResult = kafkaAdminClient
                    .createTopics(Collections.singleton(newTopic));
            createTopicsResult.values().get(topic).get();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Helper function to test producer can send messages
     *
     * @param topic      topic to send messages to
     * @param numRecords number of records to be sent
     * @throws Exception
     */
    public void doProduce(final String topic, final int numRecords) throws Exception {
        log.info("Start producing to cluster {} with bootstrap {}...", clusterArn, bootstrapBrokers);

        final Properties properties = getKafkaProducerProperties();
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < numRecords; i++) {
                log.info("Producing record " + i);
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i))).get();
            }
        }

        log.info("Finishing producing messages via Kafka.");
    }

    /**
     * Helper method to test consumption of records
     *
     * @param consumerProperties consumerProperties
     * @return
     */
    public int doConsume(final ConsumerProperties consumerProperties) {
        final Properties properties = getKafkaConsumerProperties(consumerProperties);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        return consumeRecords(consumer, consumerProperties.getTopicName()).size();
    }

    /**
     * Helper function to produce test AVRO records for Streams
     *
     * @param producerProperties producerProperties
     * @throws Exception
     */
    public List<ProducerRecord<String, Object>> doProduceAvroRecordsSerde(final ProducerProperties producerProperties,
                                                                          final List<?> records) throws Exception {
        Properties properties = getProducerProperties(producerProperties);
        final Producer<String, Object> producer =
                new KafkaProducer<>(properties, new StringSerializer(),
                                    new GlueSchemaRegistryKafkaSerializer(getMapFromPropertiesFile(properties)));
        return produceRecords(producer, producerProperties, records);
    }

    /**
     * Helper function to consume test AVRO records for Streams
     *
     * @param consumerProperties consumerProperties
     * @return
     */
    public List<ConsumerRecord<String, Object>> doConsumeAvroRecordsSerde(final ConsumerProperties consumerProperties) {
        Properties properties = getConsumerProperties(consumerProperties);
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
                new GlueSchemaRegistryKafkaDeserializer(getMapFromPropertiesFile(properties)));
        return consumeRecords(consumer, consumerProperties.getTopicName());
    }

    /**
     * Helper function to produce test AVRO records
     *
     * @param producerProperties producer properties
     * @return list of produced records
     */
    public <T> List<ProducerRecord<String, T>> doProduceRecords(final ProducerProperties producerProperties,
                                                                final List<?> records) throws Exception {
        Properties properties = getProducerProperties(producerProperties);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", GlueSchemaRegistryKafkaSerializer.class.getName());
        Producer<String, T> producer = new KafkaProducer<>(properties);

        return produceRecords(producer, producerProperties, records);
    }

    /**
     * Helper function to test consumption of records
     *
     * @param
     */
    public <T> List<ConsumerRecord<String, T>> doConsumeRecords(final ConsumerProperties consumerProperties) {
        Properties properties = getConsumerProperties(consumerProperties);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", GlueSchemaRegistryKafkaDeserializer.class.getName());
        final KafkaConsumer<String, T> consumer = new KafkaConsumer<>(properties);
        return consumeRecords(consumer, consumerProperties.getTopicName());
    }

    /**
     * Helper function to process Kafka Streams
     *
     * @param producerProperties
     */
    public void doKafkaStreamsProcess(final ProducerProperties producerProperties) throws StreamsException,
            InterruptedException {
        log.info("Start processing {} message streaming from cluster {} with bootstrap {}...",
                producerProperties.getDataFormat(), clusterArn, bootstrapBrokers);

        Properties properties = getKafkaStreamsProperties(producerProperties);
        setSchemaRegistrySerializerProperties(properties, producerProperties);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ?> source = builder.stream(producerProperties.getInputTopic());

        // Filter records whose value match to criteria of the records sent by the producer.
        final KStream<String, ?> result;
        switch (DataFormat.fromValue(producerProperties.getDataFormat())) {
            case AVRO:
                result = source
                        .filter((key, value) -> AvroGenericBackwardCompatDataGenerator.filterRecords((GenericRecord) value));
                break;
            case JSON:
                result = source
                        .filter((key, value) -> JsonSchemaGenericBackwardCompatDataGenerator.filterRecords((JsonDataWithSchema) value));
                break;
            case PROTOBUF:
                result = source.filter((key, value) -> ProtobufGenericBackwardDataGenerator.filterRecords((Message) value));
                break;
            default:
                throw new RuntimeException("Data format is not supported");

        }
        result.to(producerProperties.getOutputTopic());

        final KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp();
        streams.start();
        Thread.sleep(1000L);
        streams.close();

        log.info("Finish processing {} message streaming via Kafka.", producerProperties.getDataFormat());
    }

    private <T> List<ProducerRecord<String, T>> produceRecords(final Producer<String, T> producer,
                                                               final ProducerProperties producerProperties,
                                                               final List<?> records) throws Exception {
        log.info("Start producing to cluster {} with bootstrap {}...", clusterArn, bootstrapBrokers);
        List<ProducerRecord<String, T>> producerRecords = new ArrayList<>();

        for (int i = 0; i < records.size(); i++) {
            log.info("Fetching record {} for Kafka: {}", i, (T) records.get(i));

            final ProducerRecord<String, T> producerRecord;

            // Verify and use a unique field present in the schema as a key for the producer record.
            producerRecord = new ProducerRecord<>(producerProperties.getTopicName(), "message-" + i, (T) records.get(i));

            producerRecords.add(producerRecord);
            producer.send(producerRecord);
            Thread.sleep(500);
            log.info("Sent {} message {}",  producerProperties.getDataFormat(), i);
        }
        producer.flush();
        log.info("Successfully produced {} messages to a topic called {}", records.size(), producerProperties.getTopicName());
        return producerRecords;
    }

    private <T> List<ConsumerRecord<String, T>> consumeRecords(final KafkaConsumer<String, T> consumer,
                                                               final String topic) {
        log.info("Start consuming from cluster {} with bootstrap {} ...", clusterArn, bootstrapBrokers);

        consumer.subscribe(Collections.singleton(topic));
        List<ConsumerRecord<String, T>> consumerRecords = new ArrayList<>();
        final long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < CONSUMER_RUNTIME.toMillis()) {
            final ConsumerRecords<String, T> recordsReceived = consumer.poll(CONSUMER_RUNTIME.toMillis());
            int i = 0;
            for (final ConsumerRecord<String, T> record : recordsReceived) {
                final String key = record.key();
                final T value = record.value();
                log.info("Received message {}: key = {}, value = {}", i, key, value);
                consumerRecords.add(record);
                i++;
            }
        }

        consumer.close();
        log.info("Finished consuming messages via Kafka.");
        return consumerRecords;
    }

    /**
     * Helper function to produce test AVRO records in multithreaded manner
     *
     * @param producerProperties producerProperties
     * @return
     */
    public <T> List<ProducerRecord<String, T>> doProduceRecordsMultithreaded(final ProducerProperties producerProperties,
                                                                             final List<?> records) throws Exception {
        Properties properties = getProducerProperties(producerProperties);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", GlueSchemaRegistryKafkaSerializer.class.getName());

        int numberOfThreads = 4;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<ProducerRecord<String, T>> producerRecords = new ArrayList<>();

        for (int i = 0; i < numberOfThreads; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                Producer<String, T> producer = new KafkaProducer<>(properties);
                try {
                    producerRecords.addAll(produceRecords(producer, producerProperties, records));
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }));
        }

        CompletableFuture<Void> future =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));

        future.get();
        return producerRecords;
    }

    private Properties getProducerProperties(final ProducerProperties producerProperties) {
        Properties properties = getKafkaProducerProperties();
        setSchemaRegistrySerializerProperties(properties, producerProperties);
        return properties;
    }

    private Properties getKafkaProducerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapBrokers);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("block.on.buffer.full", false);
        properties.put("request.timeout.ms", "1000");
        return properties;
    }

    private Properties getConsumerProperties(final ConsumerProperties consumerProperties) {
        Properties properties = getKafkaConsumerProperties(consumerProperties);
        return properties;
    }

    private Properties getKafkaConsumerProperties(final ConsumerProperties consumerProperties) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapBrokers);
        properties.put("group.id", UUID.randomUUID().toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, consumerProperties.ENDPOINT);
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, consumerProperties.REGION);
        if(consumerProperties.getAvroRecordType() != null) {
            properties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, consumerProperties.getAvroRecordType());
        }
        if(consumerProperties.getProtobufMessageType() != null) {
            properties.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE,
                    consumerProperties.getProtobufMessageType());
        }
        return properties;
    }

    private Properties getKafkaStreamsProperties(final ProducerProperties producerProperties) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GlueSchemaRegistryKafkaStreamsSerde.class);
        properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, producerProperties.getDataFormat());
        if (producerProperties.getRecordType() != null) {
            if (DataFormat.PROTOBUF.name().equals(producerProperties.getDataFormat())) {
                properties.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, producerProperties.getRecordType());
            } else {
                properties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, producerProperties.getRecordType());
            }
        }
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private void setSchemaRegistrySerializerProperties(final Properties properties,
                                                       final ProducerProperties producerProperties) {
        properties.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, producerProperties.ENDPOINT);
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, producerProperties.REGION);
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, producerProperties.getSchemaName());
        properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, producerProperties.getDataFormat());
        properties.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, producerProperties.getCompressionType());
        properties.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, producerProperties.getCompatibilityType());
        properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, producerProperties.getAutoRegistrationEnabled());
    }

    /**
     * Create Config map from the properties Object passed.
     *
     * @param properties properties of configuration elements.
     * @return map of configs.
     */
    private Map<String, ?> getMapFromPropertiesFile(Properties properties) {
        return new HashMap<>(properties.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue())));
    }
}