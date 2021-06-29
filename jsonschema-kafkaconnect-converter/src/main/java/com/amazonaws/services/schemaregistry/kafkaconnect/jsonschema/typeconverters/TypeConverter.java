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

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Map;

/**
 * Convert values in Kafka Connect form into/from their logical types or plain types.
 */
public interface TypeConverter {
    JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);

    JsonNode toJson(Schema schema,
                    Object value,
                    JsonSchemaDataConfig config);

    org.everit.json.schema.Schema.Builder toJsonSchema(Schema schema,
                                                       Map<String, Object> unprocessedProperties,
                                                       JsonSchemaDataConfig jsonSchemaDataConfig);

    Object toConnect(Schema schema,
                     JsonNode value,
                     JsonSchemaDataConfig jsonSchemaDataConfig);

    SchemaBuilder toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                  JsonSchemaDataConfig jsonSchemaDataConfig);
}
