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

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaClient;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;

/**
 * Json validator
 */
public class JsonValidator {
    /**
     * Validates data against JsonSchema
     * @param schemaNode
     * @param dataNode
     */
    public void validateDataWithSchema(JsonNode schemaNode, JsonNode dataNode) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JSONObject rawSchema = new JSONObject(mapper.writeValueAsString(schemaNode));
            Schema schema = SchemaLoader.load(rawSchema, new ReferenceDisabledSchemaClient());

            switch (dataNode.getNodeType()) {
                case OBJECT: case POJO:
                    JSONObject rawObjectJson = new JSONObject(mapper.writeValueAsString(dataNode));
                    schema.validate(rawObjectJson);
                    break;
                case ARRAY:
                    JSONArray rawArrayJson = new JSONArray(mapper.writeValueAsString(dataNode));
                    schema.validate(rawArrayJson);
                    break;
                case STRING:
                    schema.validate(dataNode.textValue());
                    break;
                case NUMBER:
                    schema.validate(dataNode.numberValue());
                    break;
                case NULL:
                    schema.validate(JSONObject.NULL);
                    break;
                case BOOLEAN:
                    schema.validate(dataNode.booleanValue());
                    break;
                case BINARY:
                    schema.validate(dataNode.toString());
                    break;
                case MISSING: default:
                    throw new AWSSchemaRegistryException("JsonNodeType is unknown or unsupported: "
                            + dataNode.getNodeType());
            }
        } catch (Exception e) {
            throw new AWSSchemaRegistryException("JSON data validation against schema failed.", e);
        }
    }

    /**
     * The override SchemaClient which disables external schema reference.
     */
    public class ReferenceDisabledSchemaClient implements SchemaClient {
        @Override
        public InputStream get(final String url) {
            throw new ValidationException("Remote or local reference is not allowed: " + url);
        }
    }
}
