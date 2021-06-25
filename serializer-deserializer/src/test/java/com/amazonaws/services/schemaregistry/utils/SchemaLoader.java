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
package com.amazonaws.services.schemaregistry.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.avro.Schema;

import java.io.File;

import static org.junit.jupiter.api.Assertions.fail;

public final class SchemaLoader {
    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().setNodeFactory(JSON_NODE_FACTORY);

    /**
     * Loads and Parses schema from file.
     *
     * @param schemaFilePath Schema string
     * @return Avro schema object
     */
    public static Schema loadAvroSchema(String schemaFilePath) {
        Schema schema = null;
        try {
            schema = (new Schema.Parser()).parse(new File(schemaFilePath));
        } catch (Exception e) {
            fail("Failed to parse the avro schema file", e);
        }
        return schema;
    }

    /**
     * Loads and Parses schema from file.
     *
     * @param jsonFilePath Schema string
     * @return JSON string
     */
    public static String loadJson(String jsonFilePath) {
        String json = null;
        try {
            File file = new File(jsonFilePath);
            json = OBJECT_MAPPER.readTree(file)
                    .toString();
        } catch (Exception e) {
            fail(String.format("Failed to load the json file : ", jsonFilePath), e);
        }
        return json;
    }
}
