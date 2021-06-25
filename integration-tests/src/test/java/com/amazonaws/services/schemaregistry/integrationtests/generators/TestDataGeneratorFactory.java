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

import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory to create a new instance of test data generator.
 */
public class TestDataGeneratorFactory {
    private final ConcurrentHashMap<TestDataGeneratorType, TestDataGenerator> dataGeneratorMap =
            new ConcurrentHashMap<>();

    /**
     * Lazy initializes and returns a test data generator instance.
     *
     * @param testDataGeneratorType testDataGeneratorType
     * @return test data generator instance.
     */
    public TestDataGenerator getInstance(TestDataGeneratorType testDataGeneratorType) {
        switch (testDataGeneratorType) {
            case AVRO_GENERIC_NONE:
                this.dataGeneratorMap.computeIfAbsent(testDataGeneratorType,
                                                      key -> new AvroGenericNoneCompatDataGenerator());
                return dataGeneratorMap.get(testDataGeneratorType);
            case AVRO_GENERIC_BACKWARD:
                this.dataGeneratorMap.computeIfAbsent(testDataGeneratorType,
                                                      key -> new AvroGenericBackwardCompatDataGenerator());
                return dataGeneratorMap.get(testDataGeneratorType);
            case AVRO_SPECIFIC_NONE:
                this.dataGeneratorMap.computeIfAbsent(testDataGeneratorType,
                                                      key -> new AvroSpecificNoneCompatDataGenerator());
                return dataGeneratorMap.get(testDataGeneratorType);
            case JSON_GENERIC_NONE:
                this.dataGeneratorMap.computeIfAbsent(testDataGeneratorType,
                                                      key -> new JsonSchemaGenericNoneCompatDataGenerator());
                return dataGeneratorMap.get(testDataGeneratorType);
            case JSON_GENERIC_BACKWARD:
                this.dataGeneratorMap.computeIfAbsent(testDataGeneratorType,
                                                      key -> new JsonSchemaGenericBackwardCompatDataGenerator());
                return dataGeneratorMap.get(testDataGeneratorType);
            case JSON_SPECIFIC_NONE:
                this.dataGeneratorMap.computeIfAbsent(testDataGeneratorType,
                                                      key -> new JsonSchemaSpecificNoneCompatDataGenerator());
                return dataGeneratorMap.get(testDataGeneratorType);
            default:
                String message = String.format("Unsupported data generator type: %s", testDataGeneratorType);
                throw new AWSSchemaRegistryException(message);
        }
    }
}