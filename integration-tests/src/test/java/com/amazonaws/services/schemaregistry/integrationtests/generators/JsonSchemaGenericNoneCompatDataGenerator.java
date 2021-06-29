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
package com.amazonaws.services.schemaregistry.integrationtests.generators;

import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility functions class for the generating the Generic Records for JSON Schema
 */
public class JsonSchemaGenericNoneCompatDataGenerator implements TestDataGenerator<JsonDataWithSchema> {
    private static final String JSON_TEST_DATA_FILE_PATH = "src/test/resources/json/jsonSchemaTests.json";
    private static final String SCHEMA_FIELD_NAME = "schema";
    private static final String PAYLOAD_FIELD_NAME = "payload";
    private static final String TEST_FIELD_NAME = "test";
    private static final String VALID_FIELD_NAME = "valid";

    /**
     * Method to generate Generic JSON Records
     *
     * @return List<JsonDataWithSchema>
     */
    @Override
    public List<JsonDataWithSchema> createRecords() {
        List<JsonDataWithSchema> genericJsonRecords = new ArrayList<>();
        JsonNode jsonSchemaTestData = loadJson(JSON_TEST_DATA_FILE_PATH);
        JsonNode testNode;
        for (final JsonNode objNode : jsonSchemaTestData) {
            if (objNode.get(VALID_FIELD_NAME)
                    .asBoolean()) {
                testNode = objNode.get(TEST_FIELD_NAME);
                genericJsonRecords.add(JsonDataWithSchema.builder(testNode.get(SCHEMA_FIELD_NAME)
                                                                          .toString(), testNode.get(PAYLOAD_FIELD_NAME)
                                                                          .toString())
                                               .build());
            }
        }
        return genericJsonRecords;
    }
}
