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

package com.amazonaws.services.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroDataConfig;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

/**
 * Amazon Schema Registry Avro converter for Kafka Connect users.
 */

@Slf4j
@Data
public class AWSKafkaAvroConverter implements Converter {
    private AWSKafkaAvroConverterConfig awsKafkaAvroConverterConfig;
    private AWSKafkaAvroSerializer serializer;
    private AWSKafkaAvroDeserializer deserializer;
    private AvroData avroData;

    private boolean isKey;

    /**
     * Constructor used by Kafka Connect user.
     */
    public AWSKafkaAvroConverter() {
        serializer = new AWSKafkaAvroSerializer();
        deserializer = new AWSKafkaAvroDeserializer();
    }

    public AWSKafkaAvroConverter(
            AWSKafkaAvroSerializer awsKafkaAvroSerializer,
            AWSKafkaAvroDeserializer awsKafkaAvroDeserializer,
            AvroData avroData) {
        serializer = awsKafkaAvroSerializer;
        deserializer = awsKafkaAvroDeserializer;
        this.avroData = avroData;
    }

    /**
     * Configure the AWS Avro Converter.
     * @param configs configuration elements for the converter
     * @param isKey true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        awsKafkaAvroConverterConfig = new AWSKafkaAvroConverterConfig(configs);

        serializer.configure(configs, this.isKey);
        deserializer.configure(configs, this.isKey);

        avroData = new AvroData(new AvroDataConfig(configs));
    }

    /**
     * Convert orginal Connect data to AVRO serialized byte array
     * @param topic topic name
     * @param schema original Connect schema
     * @param value original Connect data
     * @return AVRO serialized byte array
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            Object avroValue = avroData.fromConnectData(schema, value);
            return serializer.serialize(topic, avroValue);
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * Convert AVRO serialized byte array to Connect schema and data
     * @param topic topic name
     * @param value AVRO serialized byte array
     * @return Connect schema and data
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        Object deserialized;

        if (value == null) {
            return SchemaAndValue.NULL;
        }

        try {
            deserialized = deserializer.deserialize(topic, value);
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema avroSchema = parser.parse(deserializer.getGlueSchemaRegistryDeserializationFacade().getSchemaDefinition(value));

        return avroData.toConnectData(avroSchema, deserialized);
    }
}
