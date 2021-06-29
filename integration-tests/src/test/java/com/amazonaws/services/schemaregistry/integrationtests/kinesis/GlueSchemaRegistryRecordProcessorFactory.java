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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class GlueSchemaRegistryRecordProcessorFactory implements ShardRecordProcessorFactory {
    private final RecordProcessor recordProcessor;
    private final GlueSchemaRegistryDeserializerFactory glueSchemaRegistryDeserializerFactory;
    private final GlueSchemaRegistryConfiguration gsrConfig;

    public GlueSchemaRegistryRecordProcessorFactory(RecordProcessor recordProcessor,
                                                    GlueSchemaRegistryDeserializerFactory glueSchemaRegistryDeserializerFactory,
                                                    GlueSchemaRegistryConfiguration gsrConfig) {
        this.recordProcessor = recordProcessor;
        this.glueSchemaRegistryDeserializerFactory = glueSchemaRegistryDeserializerFactory;
        this.gsrConfig = gsrConfig;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new GlueSchemaRegistryRecordProcessor(recordProcessor, glueSchemaRegistryDeserializerFactory, gsrConfig);
    }
}
