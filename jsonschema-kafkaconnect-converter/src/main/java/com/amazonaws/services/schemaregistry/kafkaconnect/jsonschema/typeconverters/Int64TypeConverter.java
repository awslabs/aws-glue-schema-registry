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
import org.everit.json.schema.NumberSchema;

import java.util.Map;

public class Int64TypeConverter implements TypeConverter {
    @Override
    public JsonNode toJson(final Schema schema,
                           final Object value,
                           final JsonSchemaDataConfig config) {
        return JSON_NODE_FACTORY.numberNode((Long) value);
    }

    @Override
    public org.everit.json.schema.Schema.Builder toJsonSchema(final Schema schema,
                                                              final Map<String, Object> unprocessedProperties,
                                                              final JsonSchemaDataConfig config) {
        String connectType = schema.type()
                .getName()
                .toLowerCase();
        org.everit.json.schema.Schema.Builder baseSchemaBuilder = NumberSchema.builder()
                .requiresInteger(true);
        unprocessedProperties.put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, connectType);
        return baseSchemaBuilder;
    }

    @Override
    public Object toConnect(final Schema schema,
                            final JsonNode value,
                            final JsonSchemaDataConfig jsonSchemaDataConfig) {
        return value.longValue();
    }

    @Override
    public SchemaBuilder toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                         JsonSchemaDataConfig jsonSchemaDataConfig) {
        return SchemaBuilder.int64();
    }
}
