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

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters.TypeConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters.TypeConverterFactory;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

/**
 * Utilities for mapping between JSON Node to Connect Value.
 */
@Slf4j
public class JsonNodeToConnectValueConverter {
    private JsonSchemaDataConfig jsonSchemaDataConfig;
    private TypeConverterFactory typeConverterFactory = new TypeConverterFactory();

    public JsonNodeToConnectValueConverter(JsonSchemaDataConfig jsonSchemaDataConfig) {
        this.jsonSchemaDataConfig = jsonSchemaDataConfig;
    }

    /**
     * Convert the given JsonNode into a Connect value object.
     * <p>
     * Using an existing implementation as reference
     * https://github.com/a0x8o/kafka/blob/9e2a20162bdedf0c42ce33665de7c82678948425/connect/json/src/main/java/org
     * /apache/kafka/connect/json/JsonConverter.java#L676
     * </p>
     *
     * @param schema    Connect Schema
     * @param jsonValue Json Value
     * @return Converted Connect Value
     */
    public Object toConnectValue(final Schema schema,
                                 final JsonNode jsonValue) {
        final Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            if (jsonValue == null || jsonValue.isNull()) {
                if (schema.defaultValue() != null) {
                    return schema.defaultValue(); // any logical type conversions should already have been applied
                }
                if (schema.isOptional()) {
                    return null;
                }
                throw new DataException("Invalid null value for required " + schemaType + " field");
            }
        } else {
            return null;
        }

        if (schema.name() != null) {
            TypeConverter logicalConverter = typeConverterFactory.get(schema.name());
            if (logicalConverter != null) {
                return logicalConverter.toConnect(schema, jsonValue, jsonSchemaDataConfig);
            }
        }

        TypeConverter typeConverter = typeConverterFactory.get(schemaType);

        return typeConverter.toConnect(schema, jsonValue, jsonSchemaDataConfig);
    }
}
