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
package com.amazonaws.services.schemaregistry.deserializers.json;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Json specific de-serializer responsible for handling the Json data format
 * specific deserialization behavior.
 */
@Slf4j
@Data
public class JsonDeserializer implements GlueSchemaRegistryDataFormatDeserializer {
    private static final GlueSchemaRegistryDeserializerDataParser DESERIALIZER_DATA_PARSER =
            GlueSchemaRegistryDeserializerDataParser.getInstance();
    private final ObjectMapper objectMapper;
    @Getter
    @Setter
    private GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs;

    /**
     * Constructor accepting various dependencies.
     *
     * @param configs configuration elements
     */
    @Builder
    public JsonDeserializer(GlueSchemaRegistryConfiguration configs) {
        this.schemaRegistrySerDeConfigs = configs;
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setNodeFactory(jsonNodeFactory);
        if (configs != null) {
            if (!CollectionUtils.isEmpty(configs.getJacksonSerializationFeatures())) {
                configs.getJacksonSerializationFeatures()
                        .forEach(this.objectMapper::enable);
            }
            if (!CollectionUtils.isEmpty(configs.getJacksonDeserializationFeatures())) {
                configs.getJacksonDeserializationFeatures()
                        .forEach(this.objectMapper::enable);
            }
        }
    }

    /**
     * Deserialize the bytes to the original JSON message given the schema retrieved
     * from the schema registry.
     *
     * @param buffer data to be de-serialized
     * @param schemaObject JSONSchema
     * @return de-serialized object
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    @Override
    public Object deserialize(@NonNull ByteBuffer buffer,
                              @NonNull Schema schemaObject) {
        try {
            String schema = schemaObject.getSchemaDefinition();
            byte[] data = DESERIALIZER_DATA_PARSER.getPlainData(buffer);

            log.debug("Length of actual message: {}", data.length);

            Object deserializedObject;

            JsonNode schemaNode = objectMapper.readTree(schema);
            JsonNode classNameNode = schemaNode.get("className");

            if (classNameNode != null) {
                String className = classNameNode.asText();
                deserializedObject = objectMapper.readValue(data, Class.forName(className));
            } else {
                JsonNode dataNode = objectMapper.readTree(data);
                deserializedObject = JsonDataWithSchema.builder(schemaNode.toString(), dataNode.toString())
                        .build();
            }

            return deserializedObject;
        } catch (IOException | ClassNotFoundException e) {
            String message = String.format("Exception occurred while de-serializing JSON message.");
            throw new AWSSchemaRegistryException(message, e);
        }
    }
}
