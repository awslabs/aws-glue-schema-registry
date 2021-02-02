package com.amazonaws.services.schemaregistry.integration_tests.kafka;

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.kafkastreams.AWSKafkaAvroSerDe;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
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

        log.info("Creating Kafka topic " + topic + " with bootstrap " + bootstrapBrokers + "...");
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
        log.info("Start producing to cluster " + clusterArn + " with bootstrap " + bootstrapBrokers + "...");

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
     * @param topic Kafka topic to consume records from
     * @return
     */
    public int doConsume(final String topic) {
        final Properties properties = getKafkaConsumerProperties();
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        return consumeAvroRecords(consumer, topic).size();
    }

    /**
     * Helper function to produce test AVRO records for Streams
     *
     * @param topic           kafka topic to send data to
     * @param endPoint        Schema Registry endpoint
     * @param region          Schema Registry region
     * @param compressionType compression algorithm
     * @return list of produced records
     * @throws Exception
     */
    public List<ProducerRecord<String, Object>> doProduceAvroRecordsSerde(final String topic, final String endPoint, final String region,
                                                                          final String compressionType) throws Exception {
        Properties properties = getProducerProperties(endPoint, region, compressionType);
        final Producer<String, Object> producer = new KafkaProducer<>(properties, new StringSerializer(),
                new AWSKafkaAvroSerializer(getMapFromPropertiesFile(properties)));
        return produceAvroRecords(producer, topic);
    }

    /**
     * Helper function to consume test AVRO records for Streams
     *
     * @param topic    kafka topic to send data to
     * @param endPoint Schema Registry endpoint
     * @param region   Schema Registry region
     * @return
     */
    public List<ConsumerRecord<String, Object>> doConsumeAvroRecordsSerde(final String topic, final String endPoint,
                                                                          final String region) {
        Properties properties = getConsumerProperties(endPoint, region);
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
                new AWSKafkaAvroDeserializer(getMapFromPropertiesFile(properties)));
        return consumeAvroRecords(consumer, topic);
    }

    /**
     * Helper function to produce test AVRO records
     *
     * @param topic           kafka topic to send data to
     * @param endPoint        Schema Registry endpoint
     * @param region          Schema Registry region
     * @param compressionType compression algorithm
     * @return list of produced records
     */
    public List<ProducerRecord<String, GenericRecord>> doProduceAvroRecords(final String topic, final String endPoint,
                                                                            final String region, final String compressionType) throws Exception {
        Properties properties = getProducerProperties(endPoint, region, compressionType);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", AWSKafkaAvroSerializer.class.getName());
        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);
        return produceAvroRecords(producer, topic);
    }

    /**
     * Helper function to test consumption of AVRO records
     *
     * @param topic    kafka topic to send data to
     * @param endPoint Schema Registry endpoint
     * @param region   Schema Registry region
     * @return list of consumed records
     */
    public List<ConsumerRecord<String, GenericRecord>> doConsumeAvroRecords(final String topic, final String endPoint,
                                                                            final String region) {
        Properties properties = getConsumerProperties(endPoint, region);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", AWSKafkaAvroDeserializer.class.getName());
        final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties);
        return consumeAvroRecords(consumer, topic);
    }

    /**
     * Helper function to process Kafka Streams
     *
     * @param inputTopic     topic to read streams from
     * @param outputTopic     topic to write streams to
     * @param endPoint        Schema Registry endpoint
     * @param region          Schema Registry region
     * @param compressionType Compression algorithm
     */
    public void doKafkaStreamsProcess(final String inputTopic, final String outputTopic, final String endPoint,
                                      final String region, final String compressionType) throws StreamsException {
        log.info("Start processing streaming message from cluster " + clusterArn + " with bootstrap " + bootstrapBrokers + "...");

        Properties properties = getKafkaStreamsProperties();
        setSchemaRegistryBasicProperties(properties, endPoint, region);
        setSchemaRegistrySerializerProperties(properties, compressionType);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, GenericRecord> source = builder.stream(inputTopic);

        // Filter records whose value match to criteria of the records sent by the producer.
        final KStream<String, GenericRecord> result = source
                .filter((key, value) -> !"11".equals(String.valueOf(value.get("id"))))
                .filter((key, value) -> !"covid-19".equals(String.valueOf(value.get("f1"))))
                .filter((key, value) -> !"morning".equals(String.valueOf(value.get("f2"))));
        result.to(outputTopic);

        final KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));

        log.info("Finish processing Avro message streaming via Kafka.");
    }

    private <T> List<ProducerRecord<String, T>> produceAvroRecords(final Producer<String, T> producer,
                                                                   final String topic) throws Exception {
        log.info("Start producing to cluster " + clusterArn + " with bootstrap " + bootstrapBrokers + "...");
        List<GenericRecord> records = Utils.generateRecordsForBackwardCompatibility();
        List<ProducerRecord<String, T>> producerRecords = new ArrayList<>();

        for (int i = 0; i < records.size(); i++) {
            GenericRecord genericRecord = records.get(i);
            log.info(String.format("Fetching Avro record %s for Kafka: %s", i, genericRecord));

            final ProducerRecord<String, T> producerRecord;

            // Verify and use a unique field present in the schema as a key for the producer record.
            producerRecord = new ProducerRecord<>(topic, genericRecord.get("id").toString(), (T) genericRecord);

            producerRecords.add(producerRecord);
            producer.send(producerRecord);
            log.info("Sent Avro message " + i);
            Thread.sleep(1000L);
        }
        producer.flush();
        log.info("Successfully produced {} messages to a topic called {}", records.size(), topic);
        log.info("Finished producing Avro messages via Kafka.");
        return producerRecords;
    }

    private <T> List<ConsumerRecord<String, T>> consumeAvroRecords(final KafkaConsumer<String, T> consumer,
                                                                   final String topic) {
        log.info("Start consuming from cluster " + clusterArn + " with bootstrap " + bootstrapBrokers + "...");

        consumer.subscribe(Collections.singleton(topic));
        List<ConsumerRecord<String, T>> consumerRecords = new ArrayList<>();
        final long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < CONSUMER_RUNTIME.toMillis()) {
            final ConsumerRecords<String, T> recordsReceived = consumer.poll(CONSUMER_RUNTIME.toMillis());
            int i = 0;
            for (final ConsumerRecord<String, T> record : recordsReceived) {
                final String key = record.key();
                final T value = record.value();
                log.info(String.format("Received Avro message %s: key = %s, value = %s", i, key, value));
                consumerRecords.add(record);
                i++;
            }
        }

        consumer.close();
        log.info("Finish consuming Avro messages via Kafka.");
        return consumerRecords;
    }

    /**
     * Helper function to produce test AVRO records in multithreaded manner
     *
     * @param topic           kafka topic to send data to
     * @param endPoint        Schema Registry endpoint
     * @param region          Schema Registry region
     * @param compressionType compression algorithm
     * @return
     */
    public List<ProducerRecord<String, GenericRecord>> doProduceAvroRecordsMultithreaded(final String topic,
                                                  final String endPoint,
                                                  final String region,
                                                  final String compressionType) throws Exception {
        Properties properties = getProducerProperties(endPoint, region, compressionType);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", AWSKafkaAvroSerializer.class.getName());

        int numberOfThreads = 4;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<ProducerRecord<String, GenericRecord>> producerRecords = new ArrayList<>();

        for (int i = 0; i < numberOfThreads; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);
                try {
                    List<ProducerRecord<String, GenericRecord>> customProducerRecords = produceAvroRecords(producer, topic);
                    producerRecords.addAll(customProducerRecords);
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

    /**
     * Helper function to produce test Specific AVRO records
     *
     * @param topic           kafka topic to send data to
     * @param endPoint        Schema Registry endpoint
     * @param region          Schema Registry region
     * @param compressionType compression algorithm
     * @return list of produced records
     */
    public List<ProducerRecord<String, Person>> doProduceSpecificAvroRecords(final String topic,
                                                                             final String endPoint,
                                                                             final String region,
                                                                             final String compressionType) throws Exception {
        Properties properties = getProducerProperties(endPoint, region, compressionType);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", AWSKafkaAvroSerializer.class.getName());
        properties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
        // Separate schema is required as evolution would fail for schema of generic records fetched from the topic
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "specific-record-schema");
        Producer<String, Person> producer = new KafkaProducer<>(properties);
        return produceSpecificAvroRecords(producer, topic);
    }

    private List<ProducerRecord<String, Person>> produceSpecificAvroRecords(final Producer<String, Person> producer,
                                                                            final String topic) throws Exception {
        log.info("Start producing to cluster {} with bootstrap {} ...", clusterArn, bootstrapBrokers);
        List<Person> records = Utils.createSpecificAvroRecords();
        List<ProducerRecord<String, Person>> producerRecords = new ArrayList<>();

        for (int i = 0; i < records.size(); i++) {
            ProducerRecord<String, Person> producerRecord = new ProducerRecord<>(topic, records.get(i)
                    .getFirstName(), records.get(i));

            producerRecords.add(producerRecord);
            producer.send(producerRecord);
            log.info("Sent Avro message " + i);
            Thread.sleep(1000L);
        }
        producer.flush();
        log.info("Successfully produced {} messages to a topic called {}", records.size(), topic);
        log.info("Finished producing Avro messages via Kafka.");
        return producerRecords;
    }

    /**
     * Helper function to test consumption of specific AVRO records
     *
     * @param topic    kafka topic to send data to
     * @param endPoint Schema Registry endpoint
     * @param region   Schema Registry region
     * @return list of consumed records
     */
    public List<ConsumerRecord<String, Person>> doConsumeSpecificAvroRecords(final String topic,
                                                                             final String endPoint,
                                                                             final String region) {
        Properties properties = getConsumerProperties(endPoint, region);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", AWSKafkaAvroDeserializer.class.getName());
        properties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
        final KafkaConsumer<String, Person> consumer = new KafkaConsumer<>(properties);
        return consumeSpecificAvroRecords(consumer, topic);
    }

    private List<ConsumerRecord<String, Person>> consumeSpecificAvroRecords(final KafkaConsumer<String, Person> consumer,
                                                                            final String topic) {
        log.info("Start consuming from cluster {} with bootstrap {} ...", clusterArn, bootstrapBrokers);

        consumer.subscribe(Collections.singleton(topic));
        List<ConsumerRecord<String, Person>> consumerRecords = new ArrayList<>();
        final long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < CONSUMER_RUNTIME.toMillis()) {
            final ConsumerRecords<String, Person> recordsReceived = consumer.poll(CONSUMER_RUNTIME.toMillis());
            int i = 0;
            for (final ConsumerRecord<String, Person> record : recordsReceived) {
                final String key = record.key();
                final Person value = record.value();
                log.info(String.format("Received Avro message %s: key = %s, value = %s", i, key, value));
                consumerRecords.add(record);
                i++;
            }
        }

        consumer.close();
        log.info("Finish consuming Avro messages via Kafka.");
        return consumerRecords;
    }

    private Properties getProducerProperties(final String endPoint, final String region, final String compressionType) {
        Properties properties = getKafkaProducerProperties();
        setSchemaRegistryBasicProperties(properties, endPoint, region);
        setSchemaRegistrySerializerProperties(properties, compressionType);
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

    private Properties getConsumerProperties(final String endPoint, final String region) {
        Properties properties = getKafkaConsumerProperties();
        setSchemaRegistryBasicProperties(properties, endPoint, region);
        return properties;
    }

    private Properties getKafkaConsumerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapBrokers);
        properties.put("group.id", UUID.randomUUID().toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private Properties getKafkaStreamsProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AWSKafkaAvroSerDe.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private void setSchemaRegistryBasicProperties(Properties properties, final String endPoint, final String region) {
        properties.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, endPoint);
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, region);
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "avro-topic");
        properties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
    }

    private void setSchemaRegistrySerializerProperties(Properties properties, final String compressionType) {
        properties.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType);
        properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
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