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

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaConverterConstants;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.StringSchema;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StringTypeConverter implements TypeConverter {
    @Override
    public JsonNode toJson(final Schema schema,
                           final Object value,
                           final JsonSchemaDataConfig config) {
        CharSequence charSeq = (CharSequence) value;
        return JSON_NODE_FACTORY.textNode(charSeq.toString());
    }

    @Override
    public org.everit.json.schema.Schema.Builder toJsonSchema(final Schema schema,
                                                              final Map<String, Object> unprocessedProperties,
                                                              final JsonSchemaDataConfig jsonSchemaDataConfig) {
        org.everit.json.schema.Schema.Builder baseSchemaBuilder;
        if (schema.parameters() != null && schema.parameters()
                .containsKey(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM)) {
            Set<Object> symbols = new HashSet<>();
            for (Map.Entry<String, String> entry : schema.parameters()
                    .entrySet()) {
                if (entry.getKey()
                        .startsWith(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + ".")) {
                    symbols.add(entry.getValue());
                }
            }
            baseSchemaBuilder = EnumSchema.builder()
                    .possibleValues(symbols);
        } else {
            baseSchemaBuilder = StringSchema.builder();
        }
        return baseSchemaBuilder;
    }

    @Override
    public Object toConnect(final Schema schema,
                            final JsonNode value,
                            final JsonSchemaDataConfig jsonSchemaDataConfig) {
        return value.textValue();
    }

    @Override
    public SchemaBuilder toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                         JsonSchemaDataConfig jsonSchemaDataConfig) {
        SchemaBuilder builder = SchemaBuilder.string();

        if (jsonSchema instanceof EnumSchema) {
            builder.parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM, null);
            List<Object> enumValues = ((EnumSchema) jsonSchema).getPossibleValuesAsList();
            enumValues.stream()
                    .forEach(e -> builder.parameter(
                            JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + "." + e.toString(), e.toString()));
        }

        return builder;
    }
}
