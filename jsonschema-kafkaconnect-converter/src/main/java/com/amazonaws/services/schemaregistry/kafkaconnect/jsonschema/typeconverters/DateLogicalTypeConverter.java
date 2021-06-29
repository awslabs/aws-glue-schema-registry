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
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.NumberSchema;

import java.util.Map;

public class DateLogicalTypeConverter implements TypeConverter {
    @Override
    public JsonNode toJson(final Schema schema,
                           final Object value,
                           final JsonSchemaDataConfig config) {
        if (!(value instanceof java.util.Date)) {
            throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
        }
        return JSON_NODE_FACTORY.numberNode(Date.fromLogical(schema, (java.util.Date) value));
    }

    @Override
    public org.everit.json.schema.Schema.Builder toJsonSchema(final Schema schema,
                                                              final Map<String, Object> unprocessedProperties,
                                                              final JsonSchemaDataConfig jsonSchemaDataConfig) {
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
        if (!(value.isInt())) {
            throw new DataException("Invalid type for Date, underlying representation should be integer but was "
                                    + value.getNodeType());
        }
        return Date.toLogical(schema, value.intValue());
    }

    @Override
    public SchemaBuilder toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                         JsonSchemaDataConfig jsonSchemaDataConfig) {
        throw new DataException("Invalid type for Date, underlying type should be integer");
    }
}
