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
package com.amazonaws.services.schemaregistry.serializers.json;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.json.ObjectMapperUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.nio.charset.StandardCharsets;

/**
 * Json serialization helper.
 */
@Slf4j
public class JsonSerializer implements GlueSchemaRegistryDataFormatSerializer {
    private static final JsonValidator JSON_VALIDATOR = new JsonValidator();
    private final JsonSchemaGenerator jsonSchemaGenerator;
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
    public JsonSerializer(GlueSchemaRegistryConfiguration configs) {
        this.schemaRegistrySerDeConfigs = configs;
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper = ObjectMapperUtils.create(configs, jsonNodeFactory);
        this.jsonSchemaGenerator = new JsonSchemaGenerator(this.objectMapper);
    }

    /**
     * Serialize the JSON object to bytes
     *
     * @param data the JSON object for serialization
     * @return the serialized byte array
     * @throws AWSSchemaRegistryException AWS Schema Registry Exception
     */
    @Override
    public byte[] serialize(Object data) {
        byte[] bytes;

        final JsonNode dataNode = getDataNode(data);
        final JsonNode schemaNode = getSchemaNode(data);
        JSON_VALIDATOR.validateDataWithSchema(schemaNode, dataNode);

        bytes = writeBytes(dataNode);
        return bytes;
    }

    private byte[] writeBytes(JsonNode dataNode) {
        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(dataNode);
        } catch (Exception e) {
            throw new AWSSchemaRegistryException(e.getMessage(), e);
        }
        return bytes;
    }

    /**
     * Checks if an object is of type generic json
     * i.e. it has schema string and data string
     *
     * @param object
     * @return
     */
    private boolean isWrapper(Object object) {
        return object instanceof JsonDataWithSchema;
    }

    /**
     * Get the schema definition.
     *
     * @param object object for which schema definition has to be derived
     * @return schema string
     */
    @Override
    public String getSchemaDefinition(@NonNull Object object) {
        JsonNode schema = getSchemaNode(object);
        return schema.toString();
    }

    private JsonNode getSchemaNode(@NonNull Object object) {
        JsonNode schemaNode;
        if (isWrapper(object)) {
            schemaNode = getSchemaNodeFromWrapperObject((JsonDataWithSchema) object);
        } else {
            try {
                schemaNode = jsonSchemaGenerator.generateJsonSchema(object.getClass());
            } catch (Exception e) {
                throw new AWSSchemaRegistryException(
                        "Could not generate schema from the type provided " + object.getClass(), e);
            }
        }

        return schemaNode;
    }

    private JsonNode getDataNode(@NonNull Object object) {
        if (isWrapper(object)) {
            return getDataNodeFromWrapperObject((JsonDataWithSchema) object);
        }
        return getDataNodeFromSpecificObject(object);
    }

    private JsonNode getSchemaNodeFromWrapperObject(JsonDataWithSchema object) {
        String schema = object.getSchema();
        return convertToJsonNode(schema);
    }

    private JsonNode getDataNodeFromWrapperObject(JsonDataWithSchema object) {
        String payload = object.getPayload();
        return convertToJsonNode(payload);
    }

    private JsonNode getDataNodeFromSpecificObject(Object object) {
        try {
            return objectMapper.valueToTree(object);
        } catch (Exception e) {
            throw new AWSSchemaRegistryException("Not a valid Specific Json Record.", e);
        }
    }

    private JsonNode convertToJsonNode(String jsonString) {
        try {
            return objectMapper.readTree(jsonString);
        } catch (JsonProcessingException e) {
            throw new AWSSchemaRegistryException("Malformed JSON", e);
        }
    }

    public void validate(String schemaDefinition, byte[] data) {
        //We assume the data bytes are encoded as UTF-8 Strings.
        //We might want to provide customization of this if required.
        final String payload = new String(data, StandardCharsets.UTF_8);

        JsonDataWithSchema jsonDataWithSchema =
            new JsonDataWithSchema(schemaDefinition, payload);

        validate(jsonDataWithSchema);
    }

    public void validate(Object jsonDataWithSchema) {
        JsonNode schemaNode = getSchemaNode(jsonDataWithSchema);
        JsonNode dataNode = getDataNode(jsonDataWithSchema);
        JSON_VALIDATOR.validateDataWithSchema(schemaNode, dataNode);
    }
}
