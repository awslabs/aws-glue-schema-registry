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
package com.amazonaws.services.schemaregistry.serializers;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.avro.AvroSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonSerializer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory to create a new instance of protocol specific serializer.
 */
@Slf4j
public class GlueSchemaRegistrySerializerFactory {
    private final ConcurrentHashMap<DataFormat, GlueSchemaRegistryDataFormatSerializer> serializerMap =
            new ConcurrentHashMap<>();

    /**
     * Lazy initializes and returns a specific de-serializer instance.
     *
     * @param dataFormat               dataFormat for creating appropriate instance
     * @param glueSchemaRegistryConfig configuration elements for serializers
     * @return protocol specific de-serializer instance.
     */
    public GlueSchemaRegistryDataFormatSerializer getInstance(@NonNull DataFormat dataFormat,
                                                              @NonNull GlueSchemaRegistryConfiguration glueSchemaRegistryConfig) {
        switch (dataFormat) {
            case AVRO:
                this.serializerMap.computeIfAbsent(dataFormat, key -> new AvroSerializer());

                log.debug("Returning Avro serializer instance from GlueSchemaRegistrySerializerFactory");
                return this.serializerMap.get(dataFormat);
            case JSON:
                this.serializerMap.computeIfAbsent(dataFormat, key -> new JsonSerializer(glueSchemaRegistryConfig));

                log.debug("Returning Json serializer instance from GlueSchemaRegistrySerializerFactory");
                return this.serializerMap.get(dataFormat);
            default:
                String message = String.format("Unsupported data format: %s", dataFormat);
                throw new AWSSchemaRegistryException(message);
        }
    }
}
