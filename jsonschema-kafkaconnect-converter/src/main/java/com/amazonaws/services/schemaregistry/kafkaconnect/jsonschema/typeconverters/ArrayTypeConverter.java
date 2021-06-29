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
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaToConnectSchemaConverter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.ArraySchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class ArrayTypeConverter implements TypeConverter {
    private ConnectValueToJsonNodeConverter connectValueToJsonNodeConverter;
    private ConnectSchemaToJsonSchemaConverter connectSchemaToJsonSchemaConverter;
    private JsonNodeToConnectValueConverter jsonNodeToConnectValueConverter;
    private JsonSchemaToConnectSchemaConverter jsonSchemaToConnectSchemaConverter;

    @Override
    public JsonNode toJson(final Schema schema,
                           final Object value,
                           final JsonSchemaDataConfig config) {
        connectValueToJsonNodeConverter = new ConnectValueToJsonNodeConverter(config);
        Collection collection = (Collection) value;
        ArrayNode list = JSON_NODE_FACTORY.arrayNode();
        for (Object elem : collection) {
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode fieldValue = connectValueToJsonNodeConverter.convertToJson(valueSchema, elem);
            list.add(fieldValue);
        }
        return list;
    }

    @Override
    public org.everit.json.schema.Schema.Builder toJsonSchema(final Schema schema,
                                                              final Map<String, Object> unprocessedProperties,
                                                              final JsonSchemaDataConfig jsonSchemaDataConfig) {
        connectSchemaToJsonSchemaConverter = new ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig);
        org.everit.json.schema.Schema.Builder baseSchemaBuilder = ArraySchema.builder()
                .allItemSchema(connectSchemaToJsonSchemaConverter.fromConnectSchema(schema.valueSchema(), false));
        return baseSchemaBuilder;
    }

    @Override
    public Object toConnect(final Schema schema,
                            final JsonNode value,
                            final JsonSchemaDataConfig jsonSchemaDataConfig) {
        jsonNodeToConnectValueConverter = new JsonNodeToConnectValueConverter(jsonSchemaDataConfig);
        Schema elemSchema = schema == null ? null : schema.valueSchema();
        ArrayList<Object> result = new ArrayList<>();
        for (JsonNode elem : value) {
            result.add(jsonNodeToConnectValueConverter.toConnectValue(elemSchema, elem));
        }
        return result;
    }

    @Override
    public SchemaBuilder toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                         JsonSchemaDataConfig jsonSchemaDataConfig) {
        jsonSchemaToConnectSchemaConverter = new JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig);
        ArraySchema arraySchema = (ArraySchema) jsonSchema;
        org.everit.json.schema.Schema itemsSchema = arraySchema.getAllItemSchema();

        if (itemsSchema == null) {
            throw new DataException("Array schema did not specify the items type");
        }

        SchemaBuilder builder = SchemaBuilder.array(jsonSchemaToConnectSchemaConverter.toConnectSchema(itemsSchema));

        return builder;
    }
}