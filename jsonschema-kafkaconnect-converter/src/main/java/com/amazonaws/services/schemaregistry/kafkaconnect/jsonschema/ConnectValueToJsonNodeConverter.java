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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

/**
 * Utilities for mapping between our runtime Connect Value to JSON Node.
 */
@Slf4j
public class ConnectValueToJsonNodeConverter {
    private static final JsonNodeFactory JSON_NODE_FACTORY = TypeConverter.JSON_NODE_FACTORY;
    private TypeConverterFactory typeConverterFactory = new TypeConverterFactory();
    private JsonSchemaDataConfig jsonSchemaDataConfig;

    public ConnectValueToJsonNodeConverter(JsonSchemaDataConfig jsonSchemaDataConfig) {
        this.jsonSchemaDataConfig = jsonSchemaDataConfig;
    }

    /**
     * This method convertToJson in JsonConverter of Apache Kafka is private and hence this was taken here with
     * simplification and moving the logic of data conversion to TypeConverters
     * <p>
     * https://github.com/apache/kafka/blob/12a1e68aeb2d1e38c06d92c957dabc239e2252d5/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java#L561
     * <p>
     * <p>
     * Converts Connect value to JsonNode
     *
     * @param schema connect schema
     * @param value  connect value
     * @return converted json Node
     */
    public JsonNode convertToJson(Schema schema,
                                  Object value) {
        if (value == null) {
            // Any schema is valid and we don't have a default, so treat this as an optional schema
            if (schema == null) {
                return JSON_NODE_FACTORY.nullNode();
            }
            if (schema.defaultValue() != null) {
                return convertToJson(schema, schema.defaultValue());
            }
            if (schema.isOptional()) {
                return JSON_NODE_FACTORY.nullNode();
            }
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        if (schema != null && schema.name() != null) {
            TypeConverter logicalConverter = typeConverterFactory.get(schema.name());
            if (logicalConverter != null) {
                return logicalConverter.toJson(schema, value, jsonSchemaDataConfig);
            }
        }

        final Schema.Type schemaType;
        try {
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null) {
                    throw new DataException(
                            "Java class " + value.getClass() + " does not have corresponding schema type.");
                }
            } else {
                schemaType = schema.type();
            }

            TypeConverter typeConverter = typeConverterFactory.get(schemaType);
            if (typeConverter != null) {
                return typeConverter.toJson(schema, value, jsonSchemaDataConfig);
            } else {
                throw new DataException("Couldn't convert " + value + " to JSON.");
            }
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type()
                    .toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }
}
