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
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;

public class JsonSchemaGenericBackwardCompatDataGenerator implements TestDataGenerator<JsonDataWithSchema> {

    private static final String JSON_TEST_DATA_FILE_PATH = "src/test/resources/json/backward/backward%d.json";
    private static final String SCHEMA_FIELD_NAME = "schema";
    private static final String PAYLOAD_FIELD_NAME = "payload";

    /**
     * Method to generate Generic JSON Records
     *
     * @return List<JsonDataWithSchema>
     */
    @Override
    public List<JsonDataWithSchema> createRecords() {
        List<JsonDataWithSchema> genericJsonRecords = new ArrayList<>();
        JsonNode jsonSchemaTestData;
        JsonNode schemaNode;
        for (int i = 1; i < 3; ++i) {
            jsonSchemaTestData = loadJson(String.format(JSON_TEST_DATA_FILE_PATH, i));
            schemaNode = jsonSchemaTestData.get(SCHEMA_FIELD_NAME);
            for (final JsonNode dataNode: jsonSchemaTestData.get(PAYLOAD_FIELD_NAME)) {
                genericJsonRecords.add(JsonDataWithSchema.builder(schemaNode.toString(), dataNode.toString()).build());
            }
        }
        return genericJsonRecords;
    }

    @SneakyThrows
    public static boolean filterRecords(JsonDataWithSchema jsonDataWithSchema) {
        String payload = jsonDataWithSchema.getPayload();
        JsonNode jsonNode = new ObjectMapper().readTree(payload);

        return !String.valueOf(jsonNode.get("f1")).contains("Stranger") || Integer.parseInt(
            String.valueOf(jsonNode.get("f2"))) != 911;
    }
}
