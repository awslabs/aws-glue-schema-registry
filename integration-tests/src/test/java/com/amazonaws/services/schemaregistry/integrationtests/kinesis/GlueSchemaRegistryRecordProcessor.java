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

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;

public class GlueSchemaRegistryRecordProcessor implements ShardRecordProcessor {
    private final Logger LOGGER = LogManager.getLogger(GlueSchemaRegistryRecordProcessor.class);

    private RecordProcessor recordProcessor;
    private GlueSchemaRegistryDeserializerFactory glueSchemaRegistryDeserializerFactory;
    private GlueSchemaRegistryConfiguration gsrConfig;
    private GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer;

    public GlueSchemaRegistryRecordProcessor(RecordProcessor recordProcessor,
                                             GlueSchemaRegistryDeserializerFactory glueSchemaRegistryDeserializerFactory,
                                             GlueSchemaRegistryConfiguration gsrConfig) {
        this.recordProcessor = recordProcessor;
        this.glueSchemaRegistryDeserializerFactory = glueSchemaRegistryDeserializerFactory;
        this.gsrConfig = gsrConfig;
        this.glueSchemaRegistryDeserializer =
                new GlueSchemaRegistryDeserializerImpl(DefaultCredentialsProvider.builder()
                                                               .build(), gsrConfig);
    }

    public void initialize(InitializationInput initializationInput) {
        this.recordProcessor.creationSuccess = true;
        LOGGER.info("Initializing GlueSchemaRegistryRecordProcessor");
    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
        this.recordProcessor.consumptionSuccess = true;
        try {
            LOGGER.info("Processing {} record(s)", processRecordsInput.records()
                    .size());
            for (KinesisClientRecord r : processRecordsInput.records()) {
                ByteBuffer bb = r.data();
                byte[] bytes = new byte[bb.remaining()];
                bb.get(bytes);

                Schema gsrSchema = glueSchemaRegistryDeserializer.getSchema(bytes);
                LOGGER.info("Consumed Schema from GSR : {}", gsrSchema.getSchemaDefinition());
                Object decodedRecord =
                        glueSchemaRegistryDeserializerFactory.getInstance(DataFormat.valueOf(gsrSchema.getDataFormat()), gsrConfig)
                                .deserialize(ByteBuffer.wrap(bytes), gsrSchema.getSchemaDefinition());

                this.recordProcessor.consumedRecords.add(decodedRecord);

                LOGGER.info("Processed record: {}", decodedRecord);
            }
        } catch (Exception e) {
            LOGGER.error("Failed while processing records. Aborting", e);
            Runtime.getRuntime()
                    .halt(1);
        }
    }

    public void leaseLost(LeaseLostInput leaseLostInput) {
        LOGGER.info("Lost lease, so terminating.");
    }

    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            LOGGER.info("Reached shard end checkpointing.");
            shardEndedInput.checkpointer()
                    .checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            LOGGER.error("Exception while checkpointing at shard end. Giving up.", e);
        }
    }

    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        try {
            LOGGER.info("Scheduler is shutting down, checkpointing.");
            shutdownRequestedInput.checkpointer()
                    .checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            LOGGER.error("Exception while checkpointing at requested shutdown. Giving up.", e);
        }
    }
}
