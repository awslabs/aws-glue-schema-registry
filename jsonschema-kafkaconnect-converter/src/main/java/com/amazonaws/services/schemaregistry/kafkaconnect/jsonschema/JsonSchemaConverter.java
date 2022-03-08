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

package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema;

import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Glue Schema Registry JSON Schema converter for Kafka Connect users.
 */

@Slf4j
@Data
public class JsonSchemaConverter implements Converter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonSchemaConverter.class);
    private ObjectMapper objectMapper = new ObjectMapper().setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private GlueSchemaRegistryKafkaSerializer serializer;
    private GlueSchemaRegistryKafkaDeserializer deserializer;
    private ConnectSchemaToJsonSchemaConverter connectSchemaToJsonSchemaConverter;
    private ConnectValueToJsonNodeConverter connectValueToJsonNodeConverter;
    private JsonSchemaToConnectSchemaConverter jsonSchemaToConnectSchemaConverter;
    private JsonNodeToConnectValueConverter jsonNodeToConnectValueConverter;

    private boolean isKey;

    /**
     * Constructor used by Kafka Connect user.
     */
    public JsonSchemaConverter() {
        this.serializer = new GlueSchemaRegistryKafkaSerializer();
        this.serializer.setUserAgentApp(UserAgents.KAFKACONNECT);

        this.deserializer = new GlueSchemaRegistryKafkaDeserializer();
        this.deserializer.setUserAgentApp(UserAgents.KAFKACONNECT);
    }

    public JsonSchemaConverter(GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer,
                               GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer) {
        this.serializer = glueSchemaRegistryKafkaSerializer;
        this.deserializer = glueSchemaRegistryKafkaDeserializer;
    }

    /**
     * Configure the JSONSchema Converter.
     *
     * @param configs configuration elements for the converter
     * @param isKey   true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs,
                          boolean isKey) {
        this.isKey = isKey;
        new JsonSchemaConverterConfig(configs);

        this.serializer.configure(configs, this.isKey);
        this.deserializer.configure(configs, this.isKey);

        if (!MapUtils.isEmpty(configs)) {
            List<String> serializationFeatures =
                    (List<String>) configs.get(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES);
            List<String> deserializationFeatures =
                    (List<String>) configs.get(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES);
            if (!CollectionUtils.isEmpty(serializationFeatures)) {
                serializationFeatures.stream()
                        .map(sf -> SerializationFeature.valueOf(sf))
                        .forEach(this.objectMapper::enable);
            }

            if (!CollectionUtils.isEmpty(deserializationFeatures)) {
                deserializationFeatures.stream()
                        .map(dsf -> DeserializationFeature.valueOf(dsf))
                        .forEach(this.objectMapper::enable);
            }
        }

        JsonSchemaDataConfig jsonSchemaDataConfigs = new JsonSchemaDataConfig(configs);

        this.connectSchemaToJsonSchemaConverter = new ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfigs);
        this.connectValueToJsonNodeConverter = new ConnectValueToJsonNodeConverter(jsonSchemaDataConfigs);
        this.jsonSchemaToConnectSchemaConverter = new JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfigs);
        this.jsonNodeToConnectValueConverter = new JsonNodeToConnectValueConverter(jsonSchemaDataConfigs);
    }

    /**
     * Convert original Connect data to JSON serialized byte array
     *
     * @param topic  topic name
     * @param schema original Connect schema
     * @param value  original Connect data
     * @return JSON serialized byte array
     */
    @Override
    public byte[] fromConnectData(String topic,
                                  Schema schema,
                                  Object value) {
        try {
            org.everit.json.schema.Schema jsonSchema = connectSchemaToJsonSchemaConverter.fromConnectSchema(schema);
            String jsonPayload = connectValueToJsonNodeConverter.convertToJson(schema, value)
                    .toString();

            Object jsonSchemaWithData = JsonDataWithSchema.builder(jsonSchema.toString(), jsonPayload)
                    .build();
            return serializer.serialize(topic, jsonSchemaWithData);
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * Convert JSON serialized byte array to Connect schema and data
     *
     * @param topic topic name
     * @param value JSON serialized byte array
     * @return Connect schema and data
     */
    @Override
    public SchemaAndValue toConnectData(String topic,
                                        byte[] value) {
        Object deserialized;

        try {
            deserialized = deserializer.deserialize(topic, value);
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        if (deserialized == null) {
            return SchemaAndValue.NULL;
        }

        String jsonSchemaString = deserializer.getGlueSchemaRegistryDeserializationFacade()
                .getSchemaDefinition(value);

        org.everit.json.schema.Schema jsonSchema = null;

        try {
            JSONObject jsonSchemaObject = new JSONObject(jsonSchemaString);
            jsonSchema = SchemaLoader.builder()
                    .schemaJson(jsonSchemaObject)
                    .build()
                    .load()
                    .build();
        } catch (Exception e) {
            throw new DataException("Failed to read JSON Schema : " + jsonSchemaString, e);
        }

        JsonNode jsonNode = null;

        if (deserialized instanceof JsonDataWithSchema) {
            JsonDataWithSchema envelope = (JsonDataWithSchema) deserialized;
            String payload = envelope.getPayload();
            try {
                jsonNode = objectMapper.readTree(payload);
            } catch (IOException e) {
                throw new DataException("Failed to read JSON Payload : " + payload, e);
            }
        } else {
            throw new DataException("JSON Deserialized data is not in envelope format.");
        }

        Schema connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema);
        Object connectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonNode);

        SchemaAndValue schemaAndValue = new SchemaAndValue(connectSchema, connectValue);
        return schemaAndValue;
    }
}
