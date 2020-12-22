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
package com.amazonaws.services.schemaregistry.examples.kds;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.joda.time.DateTime;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This is an example of how to use Glue Schema Registry (GSR) with Kinesis Data Streams Get / Put Record APIs.
 * This code is <b>not</b> applicable if you are using KCL / KPL libraries.
 * GSR is already available in KCL / KPL libraries. See, https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-kds
 */
public class PutRecordGetRecordExample {
    private static final String AVRO_USER_SCHEMA_FILE = "src/main/resources/user.avsc";
    private static AmazonKinesis kinesisClient;
    private static final Logger logger = Logger.getLogger(PutRecordGetRecordExample.class.getSimpleName());
    private static AwsCredentialsProvider awsCredentialsProvider =
        DefaultCredentialsProvider
        .builder()
        .build();
    private static GlueSchemaRegistrySerializer glueSchemaRegistrySerializer;
    private static GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer;

    public static void main(final String[] args) throws Exception {
        Options options = new Options();
        options.addOption("region", true, "Specify region");
        options.addOption("stream", true, "Specify stream");
        options.addOption("schema", true, "Specify schema");
        options.addOption("numRecords", true, "Specify number of records");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (!cmd.hasOption("stream")) {
            throw new IllegalArgumentException("Stream name needs to be provided.");
        }
        String regionName = cmd.getOptionValue("region", "us-west-2");
        String schemaName = cmd.getOptionValue("schema", "testSchema");
        String streamName = cmd.getOptionValue("stream");
        int numOfRecords = Integer.parseInt(cmd.getOptionValue("numRecords", "10"));

        //Kinesis data streams client initialization.
        kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(regionName).build();

        //Glue Schema Registry serializer initialization for the producer.
        glueSchemaRegistrySerializer =
            new GlueSchemaRegistrySerializerImpl(
                awsCredentialsProvider,
                getSchemaRegistryConfiguration(regionName)
            );

        //Glue Schema Registry de-serializer initialization for the consumer.
        glueSchemaRegistryDeserializer =
            new GlueSchemaRegistryDeserializerImpl(awsCredentialsProvider, getSchemaRegistryConfiguration(regionName));

        //Define the Glue Schema Registry schema object that will be used to encode data.
        Schema gsrSchema =
            new com.amazonaws.services.schemaregistry.common.Schema(getAvroSchema().toString(), DataFormat.AVRO.name(),
                schemaName);
        
        logger.info("Client initialization complete.");

        Date timestamp = DateTime.now().toDate();

        //Put records into Kinesis stream.
        putRecordsWithSchema(streamName, numOfRecords, gsrSchema, timestamp);

        //Start receiving records from the stream.
        getRecordsWithSchema(streamName, timestamp);
    }

    private static void getRecordsWithSchema(String streamName, Date timestamp) throws IOException {
        //Standard Kinesis code to getRecords from a Kinesis Data Stream.
        String shardIterator;
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        List<Shard> shards = new ArrayList<>();

        DescribeStreamResult streamRes;
        do {
            streamRes = kinesisClient.describeStream(describeStreamRequest);
            shards.addAll(streamRes.getStreamDescription().getShards());

            if (shards.size() > 0) {
                shards.get(shards.size() - 1).getShardId();
            }
        } while (streamRes.getStreamDescription().getHasMoreShards());

        GetShardIteratorRequest itReq = new GetShardIteratorRequest();
        itReq.setStreamName(streamName);
        itReq.setShardId(shards.get(0).getShardId());
        itReq.setTimestamp(timestamp);
        itReq.setShardIteratorType("AT_TIMESTAMP");

        GetShardIteratorResult shardIteratorResult = kinesisClient.getShardIterator(itReq);
        shardIterator = shardIteratorResult.getShardIterator();

        // Create new GetRecordsRequest with existing shardIterator.
        GetRecordsRequest recordsRequest = new GetRecordsRequest();
        recordsRequest.setShardIterator(shardIterator);
        recordsRequest.setLimit(1000);

        GetRecordsResult result = kinesisClient.getRecords(recordsRequest);

        for (Record record : result.getRecords()) {
            ByteBuffer recordAsByteBuffer = record.getData();
            GenericRecord decodedRecord = decodeRecord(recordAsByteBuffer);
            logger.info("Decoded Record: " + decodedRecord);
        }
    }

    private static void putRecordsWithSchema(String streamName, int numOfRecords, Schema gsrSchema, Date timestamp) {
        //Standard Kinesis code to putRecords into a Kinesis Data Stream.
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);

        List<PutRecordsRequestEntry> recordsRequestEntries = new ArrayList<>();

        logger.info("Putting " + numOfRecords + " into " + streamName + " with schema" + gsrSchema);
        for (int i = 0 ; i < numOfRecords ; i ++) {
            GenericRecord record = (GenericRecord) getTestRecord(i);
            byte[] recordWithSchema = encodeRecord(record, streamName, gsrSchema);
            PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
            entry.setData(ByteBuffer.wrap(recordWithSchema));
            entry.setPartitionKey(String.valueOf(timestamp.toInstant().toEpochMilli()));

            recordsRequestEntries.add(entry);
        }

        putRecordsRequest.setRecords(recordsRequestEntries);

        PutRecordsResult putRecordResult = kinesisClient.putRecords(putRecordsRequest);

        logger.info("Successfully put records: " + putRecordResult);
    }

    private static byte[] encodeRecord(GenericRecord record, String streamName, com.amazonaws.services.schemaregistry.common.Schema gsrSchema) {
        byte[] recordAsBytes = convertRecordToBytes(record);
        //Pass the GSR Schema and record payload to glueSchemaRegistrySerializer.encode method.
        byte[] recordWithSchemaHeader =
            glueSchemaRegistrySerializer.encode(streamName, gsrSchema, recordAsBytes);
        return recordWithSchemaHeader;
    }

    private static GenericRecord decodeRecord(ByteBuffer recordByteBuffer) throws IOException {

        //Copy the data to a mutable buffer.
        byte[] recordWithSchemaHeaderBytes = new byte[recordByteBuffer.remaining()];
        recordByteBuffer.get(recordWithSchemaHeaderBytes, 0, recordWithSchemaHeaderBytes.length);

        //Passing the buffer to glueSchemaRegistryDeserializer.getSchema to extract schema object.
        com.amazonaws.services.schemaregistry.common.Schema awsSchema =
            glueSchemaRegistryDeserializer.getSchema(recordWithSchemaHeaderBytes);

        //Passing the buffer to glueSchemaRegistryDeserializer.getData to extract the actual message payload.
        byte[] data = glueSchemaRegistryDeserializer.getData(recordWithSchemaHeaderBytes);

        GenericRecord genericRecord = null;
        //Convert the decoded payload and schema to Avro object.
        if (DataFormat.AVRO.name().equals(awsSchema.getDataFormat())) {
            org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(awsSchema.getSchemaDefinition());
            genericRecord = convertBytesToRecord(avroSchema, data);
        }
        return genericRecord;
    }

    private static org.apache.avro.Schema getAvroSchema() {
        //Read Avro schema object from File.
        org.apache.avro.Schema avroSchema = null;
        try {
            avroSchema = new org.apache.avro.Schema.Parser().parse(new File(AVRO_USER_SCHEMA_FILE));
        } catch (IOException e) {
            logger.warning("Error parsing Avro schema from file" + e.getMessage());
            throw new UncheckedIOException(e);
        }
        return avroSchema;
    }

    private static byte[] convertRecordToBytes(final Object record) {
        //Standard Avro code to convert records into bytes.
        ByteArrayOutputStream recordAsBytes = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(recordAsBytes, null);
        GenericDatumWriter datumWriter = new GenericDatumWriter<>(AVROUtils.getInstance().getSchema(record));
        try {
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            logger.warning("Failed to convert record to Bytes" + e.getMessage());
            throw new UncheckedIOException(e);
        }
        return recordAsBytes.toByteArray();
    }

    private static GenericRecord convertBytesToRecord(org.apache.avro.Schema avroSchema, byte[] record) {
        //Standard Avro code to convert bytes to records.
        final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(record, null);
        GenericRecord genericRecord = null;
        try {
            genericRecord = datumReader.read(null, decoder);
        } catch (IOException e) {
            logger.warning("Failed to convert bytes to record" + e.getMessage());
            throw new UncheckedIOException(e);
        }
        return genericRecord;
    }

    private static Map<String, String> getMetadata() {
        //Metadata is optionally used by GSR while auto-registering a new schema version.
        Map<String, String> metadata = new HashMap<>();
        metadata.put("event-source-1", "topic1");
        metadata.put("event-source-2", "topic2");
        metadata.put("event-source-3", "topic3");
        return metadata;
    }

    private static GlueSchemaRegistryConfiguration getSchemaRegistryConfiguration(String regionName) {
        GlueSchemaRegistryConfiguration configs = new GlueSchemaRegistryConfiguration(regionName);
        //Optional setting to enable auto-registration.
        configs.setSchemaAutoRegistrationEnabled(true);
        //Optional setting to define metadata for the schema version while auto-registering.
        configs.setMetadata(getMetadata());
        return configs;
    }

    private static Object getTestRecord(int i) {
        //Creating some sample Avro records.
        GenericRecord genericRecord;
        genericRecord = new GenericData.Record(getAvroSchema());
        genericRecord.put("name", "testName" + i);
        genericRecord.put("favorite_number", i);
        genericRecord.put("favorite_color", "color" + i);

        return genericRecord;
    }
}
