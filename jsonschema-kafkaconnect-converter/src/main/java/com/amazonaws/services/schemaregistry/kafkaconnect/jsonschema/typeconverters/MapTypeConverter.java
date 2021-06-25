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
package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters;

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.ConnectSchemaToJsonSchemaConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.ConnectValueToJsonNodeConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonNodeToConnectValueConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaConverterConstants;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaToConnectSchemaConverter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.ObjectSchema;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MapTypeConverter implements TypeConverter {
    private ConnectValueToJsonNodeConverter connectValueToJsonNodeConverter;
    private ConnectSchemaToJsonSchemaConverter connectSchemaToJsonSchemaConverter;
    private JsonNodeToConnectValueConverter jsonNodeToConnectValueConverter;
    private JsonSchemaToConnectSchemaConverter jsonSchemaToConnectSchemaConverter;

    /**
     * Map from Connect can be represented as two types - as an JSON Object or as JSON Array. The reason for an array
     * representation is to allow non-string keys that Connect allows but JSON doesn't. String keys are default key
     * types for JSON representation.
     * <p>
     * To determine the representation, ObjectMode flag is created here.
     * If ObjectMode is true, then use string keys and JSON object
     * e.g.
     * {
     * "answer":42
     * }
     * <p>
     * OR
     * <p>
     * {
     * "key":"answer",
     * "value":42
     * }
     * <p>
     * if ObjectMode is false, then use Array-encoding
     * e.g.
     * [{
     * "key": "answer",
     * "value": 42
     * }]
     * <p>
     * OR
     * <p>
     * [{
     * "key": 42,
     * "value": 42
     * }]
     *
     * @param schema connectSchema
     * @param value  connectValue
     * @param config jsonSchemaDataConfig
     * @return JsonNode converted json Object
     */
    @Override
    public JsonNode toJson(final Schema schema,
                           final Object value,
                           final JsonSchemaDataConfig config) {
        connectValueToJsonNodeConverter = new ConnectValueToJsonNodeConverter(config);
        Map<?, ?> map = (Map<?, ?>) value;
        boolean objectMode;
        if (schema == null) {
            objectMode = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    objectMode = false;
                    break;
                }
            }
        } else {
            boolean isKeySchemaStringType = Schema.Type.STRING.equals(schema.keySchema()
                                                                              .type());
            boolean isKeySchemaOptional = schema.keySchema()
                    .isOptional();

            objectMode = isKeySchemaStringType && !isKeySchemaOptional;
        }

        ObjectNode obj = null;
        ArrayNode list = null;
        if (objectMode) {
            obj = JSON_NODE_FACTORY.objectNode();
        } else {
            list = JSON_NODE_FACTORY.arrayNode();
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Schema keySchema = schema == null ? null : schema.keySchema();
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode mapKey = connectValueToJsonNodeConverter.convertToJson(keySchema, entry.getKey());
            JsonNode mapValue = connectValueToJsonNodeConverter.convertToJson(valueSchema, entry.getValue());

            if (objectMode) {
                obj.set(mapKey.asText(), mapValue);
            } else {
                list.add(JSON_NODE_FACTORY.objectNode()
                                 .setAll(new HashMap<String, JsonNode>() {{
                                     put(JsonSchemaConverterConstants.KEY_FIELD, mapKey);
                                     put(JsonSchemaConverterConstants.VALUE_FIELD, mapValue);
                                 }}));
            }
        }
        return objectMode ? obj : list;
    }

    @Override
    public org.everit.json.schema.Schema.Builder toJsonSchema(final Schema schema,
                                                              final Map<String, Object> unprocessedProperties,
                                                              final JsonSchemaDataConfig jsonSchemaDataConfig) {
        connectSchemaToJsonSchemaConverter = new ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig);
        org.everit.json.schema.Schema.Builder baseSchemaBuilder;

        Schema keySchema = schema == null ? null : schema.keySchema();

        if (keySchema != null && Schema.Type.STRING.equals(schema.keySchema()
                                                                   .type()) && !keySchema.isOptional()) {
            baseSchemaBuilder = ObjectSchema.builder()
                    .schemaOfAdditionalProperties(
                            connectSchemaToJsonSchemaConverter.fromConnectSchema(schema.valueSchema(), false));
        } else {
            org.everit.json.schema.Schema keyJsonSchema =
                    connectSchemaToJsonSchemaConverter.fromConnectSchema(schema.keySchema(), false);
            org.everit.json.schema.Schema valueJsonSchema =
                    connectSchemaToJsonSchemaConverter.fromConnectSchema(schema.valueSchema(), false);
            org.everit.json.schema.Schema mapSchema = ObjectSchema.builder()
                    .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD, keyJsonSchema)
                    .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD, valueJsonSchema)
                    .build();
            baseSchemaBuilder = ArraySchema.builder()
                    .allItemSchema(mapSchema);
        }
        unprocessedProperties.put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, schema.type()
                .getName()
                .toLowerCase());
        return baseSchemaBuilder;
    }

    @Override
    public Object toConnect(final Schema schema,
                            final JsonNode value,
                            final JsonSchemaDataConfig jsonSchemaDataConfig) {
        jsonNodeToConnectValueConverter = new JsonNodeToConnectValueConverter(jsonSchemaDataConfig);
        Schema keySchema = schema == null ? null : schema.keySchema();
        Schema valueSchema = schema == null ? null : schema.valueSchema();

        // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
        // primitive types or a complex type as a key, it will be encoded as a list of pairs. If we don't have a
        // schema, we default to encoding in a Map.
        Map<Object, Object> result = new HashMap<>();
        if (schema == null || (!keySchema.isOptional() && keySchema.type() == Schema.Type.STRING)) {
            if (!value.isObject()) throw new DataException(
                    "Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
            Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
            while (fieldIt.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIt.next();
                result.put(entry.getKey(),
                           jsonNodeToConnectValueConverter.toConnectValue(valueSchema, entry.getValue()));
            }
        } else {
            if (!value.isArray()) {
                throw new DataException(
                        "Maps with non-string fields should be encoded as JSON array of tuples, but found "
                        + value.getNodeType());
            }
            for (JsonNode entry : value) {
                if (!entry.isObject()) {
                    throw new DataException("Found invalid map entry instead of object: " + entry.getNodeType());
                }
                if (entry.size() != 2) {
                    throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                }
                result.put(jsonNodeToConnectValueConverter.toConnectValue(keySchema, entry.get(
                        JsonSchemaConverterConstants.KEY_FIELD)),
                           jsonNodeToConnectValueConverter.toConnectValue(valueSchema, entry.get(
                                   JsonSchemaConverterConstants.VALUE_FIELD)));
            }
        }
        return result;
    }

    @Override
    public SchemaBuilder toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                         JsonSchemaDataConfig jsonSchemaDataConfig) {
        jsonSchemaToConnectSchemaConverter = new JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig);

        if (jsonSchema instanceof ArraySchema) {
            ArraySchema arraySchema = (ArraySchema) jsonSchema;
            org.everit.json.schema.Schema itemsSchema = arraySchema.getAllItemSchema();

            if (itemsSchema == null) {
                throw new DataException("Array schema did not specify the items type");
            }

            ObjectSchema objectSchema = (ObjectSchema) itemsSchema;
            Schema keySchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(objectSchema.getPropertySchemas()
                                                                                          .get(JsonSchemaConverterConstants.KEY_FIELD));
            Schema valueSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(objectSchema.getPropertySchemas()
                                                                                            .get(JsonSchemaConverterConstants.VALUE_FIELD));
            return SchemaBuilder.map(keySchema, valueSchema);
        }

        if (jsonSchema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) jsonSchema;
            Schema keySchema = Schema.STRING_SCHEMA;
            Schema valueSchema =
                    jsonSchemaToConnectSchemaConverter.toConnectSchema(objectSchema.getSchemaOfAdditionalProperties());
            return SchemaBuilder.map(keySchema, valueSchema);
        }

        throw new DataException("Json Schema for Connect Map translation should be either Object or Array Schema.");
    }
}