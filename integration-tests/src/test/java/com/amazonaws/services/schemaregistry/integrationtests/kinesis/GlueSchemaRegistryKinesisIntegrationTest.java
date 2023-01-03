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

package com.amazonaws.services.schemaregistry.integrationtests.kinesis;

import cloud.localstack.Constants;
import cloud.localstack.ServiceName;
import cloud.localstack.awssdkv2.TestUtils;
import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorFactory;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorType;
import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.Message;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = {ServiceName.KINESIS, ServiceName.DYNAMO, ServiceName.CLOUDWATCH}, imageName =
        "public.ecr.aws/d4c7g6k3/localstack", imageTag = "0.12.10")
public class GlueSchemaRegistryKinesisIntegrationTest {
    private static final Logger LOGGER = LogManager.getLogger(GlueSchemaRegistryKinesisIntegrationTest.class);
    private static final DynamoDbAsyncClient dynamoClient = TestUtils.getClientDyanamoAsyncV2();
    private static final CloudWatchAsyncClient cloudWatchClient = TestUtils.getClientCloudWatchAsyncV2();
    private static final String LOCALSTACK_HOSTNAME = "localhost";
    private static final int LOCALSTACK_KINESIS_PORT = 4566;
    private static final int LOCALSTACK_CLOUDWATCH_PORT = Constants.DEFAULT_PORTS.get(ServiceName.CLOUDWATCH)
            .intValue();
    private static final int KCL_SCHEDULER_START_UP_WAIT_TIME_SECONDS = 15;
    private static final int KCL_SCHEDULER_SHUT_DOWN_WAIT_TIME_SECONDS = 5;

    private static final String SCHEMA_REGISTRY_ENDPOINT_OVERRIDE = GlueSchemaRegistryConnectionProperties.ENDPOINT;
    private static final String REGION = GlueSchemaRegistryConnectionProperties.REGION;
    private static final String TEST_KINESIS_STREAM_PREFIX = "gsr-integ-test-kinesis-stream-";

    // Testing with single shard - can be increased but will require code changes to iterate from multiple shard Ids
    private static final int SHARD_COUNT = 1;
    private static final List<AvroRecordType> RECORD_TYPES = Arrays.stream(AvroRecordType.values())
            .filter(r -> !r.equals(AvroRecordType.UNKNOWN))
            .collect(Collectors.toList());
    private static final List<Compatibility> COMPATIBILITIES = Compatibility.knownValues()
            .stream()
            .filter(c -> c.toString()
                    .equals("NONE")) // TODO : Add Compatibility Tests for multiple compatibilities
            .collect(Collectors.toList());
    private static KinesisAsyncClient kinesisClient;
    private static String streamName = null;
    private static GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer;
    private static AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder()
            .build();

    private static List<String> schemasToCleanUp = new ArrayList<>();
    private final TestDataGeneratorFactory testDataGeneratorFactory = new TestDataGeneratorFactory();
    private final GlueSchemaRegistrySerializerFactory glueSchemaRegistrySerializerFactory =
            new GlueSchemaRegistrySerializerFactory();
    private final GlueSchemaRegistryDeserializerFactory glueSchemaRegistryDeserializerFactory =
            new GlueSchemaRegistryDeserializerFactory();

    private static Map<String, String> getMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("event-source-1", "stream1");
        metadata.put("event-source-2", "stream2");
        return metadata;
    }

    private static Stream<Arguments> testArgumentsProvider() {
        Stream.Builder<Arguments> argumentBuilder = Stream.builder();
        for (DataFormat dataFormat : DataFormat.knownValues()
                .stream()
                .collect(Collectors.toList())) {
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

    private static GlueSchemaRegistryConfiguration getSchemaRegistryConfiguration(Compatibility compatibility,
                                                                                  AWSSchemaRegistryConstants.COMPRESSION compression,
                                                                                  AvroRecordType avroRecordType, DataFormat dataFormat) {
        GlueSchemaRegistryConfiguration configs = new GlueSchemaRegistryConfiguration(REGION);
        configs.setEndPoint(SCHEMA_REGISTRY_ENDPOINT_OVERRIDE);
        configs.setSchemaAutoRegistrationEnabled(true);
        configs.setMetadata(getMetadata());
        if (dataFormat.equals(DataFormat.PROTOBUF)) {
            configs.setProtobufMessageType(ProtobufMessageType.DYNAMIC_MESSAGE);
        } else {
            configs.setAvroRecordType(avroRecordType);
        }
        configs.setCompatibilitySetting(compatibility);
        configs.setCompressionType(compression);
        return configs;
    }

    @AfterAll
    public static void tearDown() throws URISyntaxException {
        LOGGER.info("Starting Clean-up of schemas created with GSR.");
        GlueClient glueClient = GlueClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(REGION))
                .endpointOverride(new URI(SCHEMA_REGISTRY_ENDPOINT_OVERRIDE))
                .build();

        for (String schemaName : schemasToCleanUp) {
            LOGGER.info("Cleaning up schema {}..", schemaName);
            DeleteSchemaRequest deleteSchemaRequest = DeleteSchemaRequest.builder()
                    .schemaId(SchemaId.builder()
                                      .registryName("default-registry")
                                      .schemaName(schemaName)
                                      .build())
                    .build();

            glueClient.deleteSchema(deleteSchemaRequest);
        }

        LOGGER.info("Finished Cleaning up {} schemas created with GSR.", schemasToCleanUp.size());
    }

    private static Stream<Arguments> testSingleKCLKPLDataProvider() {
        return DataFormat.knownValues().stream().map(Arguments::of);
    }

    @BeforeEach
    public void setUp() throws InterruptedException, ExecutionException {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        kinesisClient = TestUtils.getClientKinesisAsyncV2();

        streamName = String.format("%s%s", TEST_KINESIS_STREAM_PREFIX, RandomStringUtils.randomAlphanumeric(4));
        LOGGER.info("Creating Kinesis Stream : {} with {} shards on localStack..", streamName, SHARD_COUNT);

        CreateStreamRequest createStreamRequest = CreateStreamRequest.builder()
                .streamName(streamName)
                .shardCount(SHARD_COUNT)
                .build();
        kinesisClient.createStream(createStreamRequest)
                .get();
        Awaitility.await()
                .until(() -> StreamStatus.ACTIVE.equals(kinesisClient.describeStream(DescribeStreamRequest.builder()
                                                                                             .streamName(streamName)
                                                                                             .build())
                                                                .get()
                                                                .streamDescription()
                                                                .streamStatus()));
        LOGGER.info("Finished creating Kinesis Stream : {}", streamName, SHARD_COUNT);
    }

    @Test
    public void testKinesisProduceConsume() throws Exception {
        LOGGER.info("Starting the test for producing/consuming messages on Kinesis ...");

        String message = "Hello World";

        Instant timestamp = Instant.now();

        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(String.valueOf(timestamp.toEpochMilli()))
                .data(SdkBytes.fromUtf8String(message))
                .build();
        String shardId = kinesisClient.putRecord(putRecordRequest)
                .get()
                .shardId();

        assertNotNull(shardId);

        GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder()
                .streamName(streamName)
                .shardId(shardId)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build();

        String shardIterator = kinesisClient.getShardIterator(getShardIteratorRequest)
                .get()
                .shardIterator();

        GetRecordsRequest getRecordRequest = GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .build();
        GetRecordsResponse recordsResponse = kinesisClient.getRecords(getRecordRequest)
                .get();

        List<String> records = recordsResponse.records()
                .stream()
                .map(r -> r.data()
                        .asUtf8String())
                .collect(Collectors.toList());

        assertEquals(records.size(), 1);
        assertEquals(message, records.get(0));

        LOGGER.info("Finished test for producing/consuming messages on Kinesis.");
    }

    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    public void testKinesisProduceConsumeWithGlueSchemaRegistry(final DataFormat dataFormat,
                                                                final AvroRecordType recordType,
                                                                final Compatibility compatibility,
                                                                final AWSSchemaRegistryConstants.COMPRESSION compression) throws Exception {
        LOGGER.info("Starting test for producing/consuming messages on Kinesis with Glue Schema Registry");

        TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility));
        List<?> producerRecords = testDataGenerator.createRecords();

        GlueSchemaRegistryConfiguration gsrConfig =
                getSchemaRegistryConfiguration(compatibility, compression, recordType, dataFormat);

        String shardId =
                produceRecordsWithKinesisSDK(streamName, producerRecords, dataFormat, compatibility, gsrConfig);

        List<Object> consumerRecords = consumeRecordsWithKinesisSDK(streamName, shardId, dataFormat, gsrConfig);

        assertNotEquals(0, consumerRecords.size());
        assertEquals(producerRecords.size(), consumerRecords.size());
        assertKinesisRecords(dataFormat, producerRecords.toArray(), consumerRecords.toArray());

        LOGGER.info("Finished test for producing/consuming messages on Kinesis with Glue Schema Registry");
    }

    @ParameterizedTest
    @MethodSource("testArgumentsProvider")
    public void testProduceConsumeWithKPLAndKCL(final DataFormat dataFormat,
                                                final AvroRecordType recordType,
                                                final Compatibility compatibility,
                                                final AWSSchemaRegistryConstants.COMPRESSION compression) throws Exception {
        LOGGER.info("Starting test for producing/consuming messages on Kinesis Producer Library with Glue Schema "
                    + "Registry");

        TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
                TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility));
        List<?> producerRecords = testDataGenerator.createRecords();

        GlueSchemaRegistryConfiguration gsrConfig =
                getSchemaRegistryConfiguration(compatibility, compression, recordType, dataFormat);

        RecordProcessor recordProcessor = new RecordProcessor();
        Scheduler scheduler = startConsumingWithKCL(gsrConfig, recordProcessor);

        produceRecordsWithKPL(streamName, producerRecords, dataFormat, compatibility, gsrConfig);

        TimeUnit.SECONDS.sleep(KCL_SCHEDULER_SHUT_DOWN_WAIT_TIME_SECONDS);
        scheduler.shutdown();

        assertTrue(recordProcessor.creationSuccess);
        assertTrue(recordProcessor.consumptionSuccess);
        assertKinesisRecords(dataFormat, producerRecords.toArray(), recordProcessor.consumedRecords.toArray());

        LOGGER.info("Finished test for producing/consuming messages on Kinesis Producer Library with Glue Schema "
                    + "Registry");
    }

    //Used for Canary tests.
    @ParameterizedTest
    @MethodSource("testSingleKCLKPLDataProvider")
    public void testProduceConsumeSingleRecordWithKPLAndKCL(DataFormat dataFormat) throws Exception {
        Compatibility compatibility = Compatibility.NONE;
        AvroRecordType recordType = AvroRecordType.GENERIC_RECORD;
        AWSSchemaRegistryConstants.COMPRESSION compression = AWSSchemaRegistryConstants.COMPRESSION.NONE;

        TestDataGenerator testDataGenerator = testDataGeneratorFactory.getInstance(
            TestDataGeneratorType.valueOf(dataFormat, recordType, compatibility));
        List<?> producerRecords = Collections.singletonList(testDataGenerator.createRecords().get(0));

        GlueSchemaRegistryConfiguration gsrConfig =
            getSchemaRegistryConfiguration(compatibility, compression, recordType, dataFormat);

        RecordProcessor recordProcessor = new RecordProcessor();
        Scheduler scheduler = startConsumingWithKCL(gsrConfig, recordProcessor);

        produceRecordsWithKPL(streamName, producerRecords, dataFormat, compatibility, gsrConfig);

        TimeUnit.SECONDS.sleep(KCL_SCHEDULER_SHUT_DOWN_WAIT_TIME_SECONDS);
        scheduler.shutdown();

        assertTrue(recordProcessor.creationSuccess);
        assertTrue(recordProcessor.consumptionSuccess);
        assertEquals(producerRecords.size(), recordProcessor.consumedRecords.size());
    }

    private String produceRecordsWithKinesisSDK(String streamName,
                                                List<?> producerRecords,
                                                DataFormat dataFormat,
                                                Compatibility compatibility,
                                                GlueSchemaRegistryConfiguration gsrConfig) throws InterruptedException, ExecutionException {
        GlueSchemaRegistrySerializer glueSchemaRegistrySerializer =
                new GlueSchemaRegistrySerializerImpl(awsCredentialsProvider, gsrConfig);
        GlueSchemaRegistryDataFormatSerializer dataFormatSerializer =
                glueSchemaRegistrySerializerFactory.getInstance(dataFormat, gsrConfig);

        String shardId = null;
        Instant timestamp = Instant.now();

        String schemaName = String.format("%s-%s-%s", streamName, dataFormat.name(), compatibility);
        schemasToCleanUp.add(schemaName);

        for (int i = 0; i < producerRecords.size(); i++) {
            Object record = producerRecords.get(i);
            Schema gsrSchema =
                    new Schema(dataFormatSerializer.getSchemaDefinition(record), dataFormat.name(), schemaName);

            byte[] serializedBytes = dataFormatSerializer.serialize(record);

            byte[] gsrEncodedBytes = glueSchemaRegistrySerializer.encode(streamName, gsrSchema, serializedBytes);

            PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                    .streamName(streamName)
                    .partitionKey(String.valueOf(timestamp.toEpochMilli()))
                    .data(SdkBytes.fromByteArray(gsrEncodedBytes))
                    .build();
            shardId = kinesisClient.putRecord(putRecordRequest)
                    .get()
                    .shardId();

            assertNotNull(shardId);
        }
        return shardId;
    }

    private List<Object> consumeRecordsWithKinesisSDK(String streamName,
                                                      String shardId,
                                                      DataFormat dataFormat,
                                                      GlueSchemaRegistryConfiguration gsrConfig) throws InterruptedException, ExecutionException {
        glueSchemaRegistryDeserializer = new GlueSchemaRegistryDeserializerImpl(awsCredentialsProvider, gsrConfig);

        GlueSchemaRegistryDataFormatDeserializer gsrDataFormatDeserializer =
                glueSchemaRegistryDeserializerFactory.getInstance(dataFormat, gsrConfig);

        GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder()
                .streamName(streamName)
                .shardId(shardId)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build();

        String shardIterator = kinesisClient.getShardIterator(getShardIteratorRequest)
                .get()
                .shardIterator();

        GetRecordsRequest getRecordRequest = GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .build();
        GetRecordsResponse recordsResponse = kinesisClient.getRecords(getRecordRequest)
                .get();

        List<Object> consumerRecords = new ArrayList<>();
        List<Record> recordsFromKinesis = recordsResponse.records();

        for (int i = 0; i < recordsFromKinesis.size(); i++) {
            byte[] consumedBytes = recordsFromKinesis.get(i)
                    .data()
                    .asByteArray();

            Schema gsrSchema = glueSchemaRegistryDeserializer.getSchema(consumedBytes);
            LOGGER.info("Consumed Schema from GSR : {}", gsrSchema.getSchemaDefinition());
            Object decodedRecord = gsrDataFormatDeserializer.deserialize(ByteBuffer.wrap(consumedBytes),
                                                                         gsrSchema);
            consumerRecords.add(decodedRecord);
        }

        LOGGER.info("Decoded {} Records from Kinesis Stream {}", consumerRecords.size(), streamName);

        return consumerRecords;
    }

    private String produceRecordsWithKPL(String streamName,
                                         List<?> producerRecords,
                                         DataFormat dataFormat,
                                         Compatibility compatibility,
                                         GlueSchemaRegistryConfiguration gsrConfig) throws InterruptedException,
            ExecutionException {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration().setRegion(REGION)
                .setKinesisEndpoint(LOCALSTACK_HOSTNAME)
                .setKinesisPort(LOCALSTACK_KINESIS_PORT)
                .setCloudwatchEndpoint(LOCALSTACK_HOSTNAME)
                .setCloudwatchPort(LOCALSTACK_CLOUDWATCH_PORT)
                .setVerifyCertificate(false)
                .setAggregationEnabled(false)
                .setLogLevel(Level.ERROR.name()
                                     .toLowerCase())
                .setGlueSchemaRegistryConfiguration(gsrConfig);

        final KinesisProducer producer = new KinesisProducer(config);

        GlueSchemaRegistryDataFormatSerializer dataFormatSerializer =
                glueSchemaRegistrySerializerFactory.getInstance(dataFormat, gsrConfig);

        List<Future<UserRecordResult>> putFutures = new LinkedList<>();

        Instant timestamp = Instant.now();
        String schemaName = String.format("%s-%s-%s", streamName, dataFormat.name(), compatibility);
        schemasToCleanUp.add(schemaName);

        for (int i = 0; i < producerRecords.size(); i++) {
            Object record = producerRecords.get(i);
            Schema gsrSchema =
                    new Schema(dataFormatSerializer.getSchemaDefinition(record), dataFormat.name(), schemaName);

            byte[] serializedBytes = dataFormatSerializer.serialize(record);

            putFutures.add(producer.addUserRecord(streamName, Long.toString(timestamp.toEpochMilli()), null,
                                                  ByteBuffer.wrap(serializedBytes), gsrSchema));
        }

        String shardId = null;

        for (Future<UserRecordResult> future : putFutures) {
            UserRecordResult userRecordResult = future.get();
            shardId = userRecordResult.getShardId();

            assertTrue(userRecordResult.isSuccessful());
            assertNotNull(userRecordResult.getShardId());
        }

        return shardId;
    }

    private Scheduler startConsumingWithKCL(GlueSchemaRegistryConfiguration gsrConfig,
                                            RecordProcessor recordProcessor) throws InterruptedException {
        GlueSchemaRegistryRecordProcessorFactory glueSchemaRegistryRecordProcessorFactory =
                new GlueSchemaRegistryRecordProcessorFactory(recordProcessor, glueSchemaRegistryDeserializerFactory,
                                                             gsrConfig);

        ConfigsBuilder configsBuilder =
                new ConfigsBuilder(streamName, streamName, kinesisClient, dynamoClient, cloudWatchClient, streamName,
                                   glueSchemaRegistryRecordProcessorFactory);
        RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig()
                .retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient));

        Scheduler scheduler = new Scheduler(configsBuilder.checkpointConfig(), configsBuilder.coordinatorConfig(),
                                            configsBuilder.leaseManagementConfig(), configsBuilder.lifecycleConfig(),
                                            configsBuilder.metricsConfig()
                                                    .metricsFactory(new NullMetricsFactory()),
                                            configsBuilder.processorConfig(), retrievalConfig);

        new Thread(scheduler).start();

        TimeUnit.SECONDS.sleep(KCL_SCHEDULER_START_UP_WAIT_TIME_SECONDS);

        return scheduler;
    }

    private void assertKinesisRecords(DataFormat dataFormat, Object[] expected, Object[] actual) {
        assertEquals(expected.length, actual.length);

        if (dataFormat.equals(DataFormat.PROTOBUF)) {
            Function<Object, byte[]> messageToBytes = object -> ((Message) object).toByteArray();
            Object [] expectedByteArray = Arrays.stream(expected).map(messageToBytes).toArray();
            Object [] actualBytesArray = Arrays.stream(actual).map(messageToBytes).toArray();

            assertArrayEquals(expectedByteArray, actualBytesArray);
        } else {
            assertArrayEquals(expected, actual);
        }
    }
}
