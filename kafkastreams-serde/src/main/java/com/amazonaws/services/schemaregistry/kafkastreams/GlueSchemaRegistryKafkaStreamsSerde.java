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

package com.amazonaws.services.schemaregistry.kafkastreams;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Amazon Glue Schema Registry serializer and de-serializer wrapper for Kafka Streams users.
 */
public class GlueSchemaRegistryKafkaStreamsSerde implements Serde<Object> {

    private final Serde<Object> inner;

    /**
     * Constructor used by Kafka Streams user.
     */
    public GlueSchemaRegistryKafkaStreamsSerde() {
        inner = Serdes.serdeFrom(new GlueSchemaRegistryKafkaSerializer(), new GlueSchemaRegistryKafkaDeserializer());
    }

    public GlueSchemaRegistryKafkaStreamsSerde(GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer,
                                               GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer) {
        inner = Serdes.serdeFrom(glueSchemaRegistryKafkaSerializer, glueSchemaRegistryKafkaDeserializer);
    }

    /**
     * Get the serializer.
     * @return an GlueSchemaRegistryKafkaSerializer instance.
     */
    @Override
    public Serializer<Object> serializer() {
        return inner.serializer();
    }

    /**
     * Get the de-serializer.
     * @return an GlueSchemaRegistryKafkaDeserializer instance.
     */
    @Override
    public Deserializer<Object> deserializer() {
        return inner.deserializer();
    }

    /**
     * Configure the serializer and de-serializer wrapper.
     * @param serdeConfig configuration elements for the wrapper
     * @param isSerdeForRecordKeys true if key, false otherwise
     */
    @Override
    public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
        inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
        inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
    }

    /**
     * Resource clean up for Closeable.
     */
    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }
}
