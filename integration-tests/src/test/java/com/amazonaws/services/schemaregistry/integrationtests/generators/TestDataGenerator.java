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

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.List;

public interface TestDataGenerator<T> {
    List<T> createRecords() throws Exception;

    default JsonNode loadJson(String jsonFilePath) {
        try {
            File file = new File(jsonFilePath);
            return new ObjectMapper().readTree(file);
        } catch (Exception e) {
            throw new AWSSchemaRegistryException(String.format("Failed to load the json file : ", jsonFilePath), e);
        }
    }
}
