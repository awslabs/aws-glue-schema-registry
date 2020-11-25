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
package com.amazonaws.services.schemaregistry.deserializers;

import com.amazonaws.services.schemaregistry.common.AWSDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSAvroDeserializer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory to create a new instance of protocol specific de-serializer.
 */
@Slf4j
public class AWSDeserializerFactory {
    private final
    ConcurrentHashMap<DataFormat, AWSDataFormatDeserializer> deserializerMap = new ConcurrentHashMap<>();

    /**
     * Constructor for de-serialization implementations.
     *
     */
    public AWSDeserializerFactory() {

    }

    /**
     * Lazy initializes and returns a specific de-serializer instance.
     *
     * @param dataFormat dataFormat for creating appropriate instance
     * @param configs    configuration elements for de-serializers
     * @return protocol specific de-serializer instance.
     */
    public AWSDataFormatDeserializer getInstance(@NonNull DataFormat dataFormat,
                                                 @NonNull GlueSchemaRegistryConfiguration configs) {
        switch (dataFormat) {
            case AVRO:
                // Lazy initialization of de-serializer instances
                this.deserializerMap.computeIfAbsent(dataFormat, key -> {
                    AWSAvroDeserializer awsAvroDeserializer = AWSAvroDeserializer
                            .builder()
                            .configs(configs)
                            .build();

                    return awsAvroDeserializer;
                });

                log.debug("Returning Avro de-serializer instance from AWSDeserializerFactory");
                return this.deserializerMap.get(dataFormat);
            default:
                String message = String.format("Data Format is not supported %s", dataFormat);
                throw new UnsupportedOperationException(message);
        }
    }
}
