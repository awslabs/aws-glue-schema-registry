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

package com.amazonaws.services.schemaregistry.common.compatibility;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Compatibility;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonSchemaCompatibilityCheckerTest {

    private JsonSchemaCompatibilityChecker checker;

    @BeforeEach
    void setUp() {
        checker = new JsonSchemaCompatibilityChecker();
    }

    @Nested
    @DisplayName("FORWARD Compatibility")
    class ForwardCompatibilityTests {

        @Test
        @DisplayName("Making required field optional should FAIL")
        void makingRequiredFieldOptional_shouldFail() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.FORWARD);

            assertFalse(errors.isEmpty(), "Should detect FORWARD incompatibility");
            assertTrue(errors.get(0).contains("name"), "Error should mention the field name");
            assertTrue(errors.get(0).contains("FORWARD"), "Error should mention FORWARD");
        }

        @Test
        @DisplayName("Removing field from required array should FAIL")
        void removingFromRequired_shouldFail() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"}},\"required\":[\"a\",\"b\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"}},\"required\":[\"a\"]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.FORWARD);

            assertFalse(errors.isEmpty());
            assertTrue(errors.get(0).contains("b"));
        }

        @Test
        @DisplayName("Adding new required field should PASS")
        void addingRequiredField_shouldPass() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}},\"required\":[\"a\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"}},\"required\":[\"a\",\"b\"]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.FORWARD);

            assertTrue(errors.isEmpty(), "Adding required field should pass FORWARD: " + errors);
        }

        @Test
        @DisplayName("No changes should PASS")
        void noChanges_shouldPass() {
            String schema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";

            List<String> errors = checker.checkCompatibility(schema, schema, Compatibility.FORWARD);

            assertTrue(errors.isEmpty());
        }
    }

    @Nested
    @DisplayName("BACKWARD Compatibility")
    class BackwardCompatibilityTests {

        @Test
        @DisplayName("Adding required field should FAIL")
        void addingRequiredField_shouldFail() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}},\"required\":[\"a\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"}},\"required\":[\"a\",\"b\"]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.BACKWARD);

            assertFalse(errors.isEmpty(), "Should detect BACKWARD incompatibility");
            assertTrue(errors.get(0).contains("b"));
            assertTrue(errors.get(0).contains("BACKWARD"));
        }

        @Test
        @DisplayName("Making required field optional should PASS")
        void makingRequiredFieldOptional_shouldPass() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.BACKWARD);

            assertTrue(errors.isEmpty(), "Making field optional should pass BACKWARD: " + errors);
        }

        @Test
        @DisplayName("Adding optional field should PASS")
        void addingOptionalField_shouldPass() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}},\"required\":[\"a\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"}},\"required\":[\"a\"]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.BACKWARD);

            assertTrue(errors.isEmpty());
        }
    }

    @Nested
    @DisplayName("FULL Compatibility")
    class FullCompatibilityTests {

        @Test
        @DisplayName("Making required field optional should FAIL (violates FORWARD)")
        void makingRequiredOptional_shouldFail() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.FULL);

            assertFalse(errors.isEmpty());
            assertTrue(errors.stream().anyMatch(e -> e.contains("FORWARD")));
        }

        @Test
        @DisplayName("Adding required field should FAIL (violates BACKWARD)")
        void addingRequiredField_shouldFail() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}},\"required\":[\"a\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"}},\"required\":[\"a\",\"b\"]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.FULL);

            assertFalse(errors.isEmpty());
            assertTrue(errors.stream().anyMatch(e -> e.contains("BACKWARD")));
        }

        @Test
        @DisplayName("Adding optional field should PASS")
        void addingOptionalField_shouldPass() {
            String v1 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}},\"required\":[\"a\"]}";
            String v2 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"}},\"required\":[\"a\"]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.FULL);

            assertTrue(errors.isEmpty(), "Adding optional field should pass FULL: " + errors);
        }
    }

    @Nested
    @DisplayName("Nested Definitions")
    class NestedDefinitionsTests {

        @Test
        @DisplayName("FORWARD: Making nested required field optional should FAIL")
        void nestedRequiredToOptional_shouldFailForward() {
            String v1 = "{\"type\":\"object\",\"definitions\":{\"Address\":{\"type\":\"object\",\"required\":[\"street\",\"city\"]}}}";
            String v2 = "{\"type\":\"object\",\"definitions\":{\"Address\":{\"type\":\"object\",\"required\":[\"city\"]}}}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.FORWARD);

            assertFalse(errors.isEmpty());
            assertTrue(errors.get(0).contains("street"));
            assertTrue(errors.get(0).contains("Address") || errors.get(0).contains("definitions"));
        }
    }

    @Nested
    @DisplayName("Real-World Case: Employee Schema")
    class RealWorldTests {

        // Simplified version of the actual customer schema
        private static final String V1_ADDRESS_REQUIRED = 
            "{\"definitions\":{\"Address\":{\"type\":\"object\"," +
            "\"properties\":{\"streetNumber\":{\"type\":\"number\"},\"city\":{\"type\":\"string\"}}," +
            "\"required\":[\"streetNumber\",\"city\"]}}}";

        private static final String V2_ADDRESS_OPTIONAL_STREET = 
            "{\"definitions\":{\"Address\":{\"type\":\"object\"," +
            "\"properties\":{\"streetNumber\":{\"type\":\"number\"},\"city\":{\"type\":\"string\"}}," +
            "\"required\":[\"city\"]}}}";

        @Test
        @DisplayName("Real bug: streetNumber removed from required should FAIL FORWARD")
        void streetNumberRemovedFromRequired_shouldFailForward() {
            List<String> errors = checker.checkCompatibility(V2_ADDRESS_OPTIONAL_STREET, V1_ADDRESS_REQUIRED, Compatibility.FORWARD);

            assertFalse(errors.isEmpty(), 
                "Removing streetNumber from required should fail FORWARD compatibility. " +
                "This is the exact bug that AWS Glue incorrectly allows.");
            assertTrue(errors.get(0).contains("streetNumber"));
        }

        @Test
        @DisplayName("Real bug: streetNumber removed from required should PASS BACKWARD")
        void streetNumberRemovedFromRequired_shouldPassBackward() {
            List<String> errors = checker.checkCompatibility(V2_ADDRESS_OPTIONAL_STREET, V1_ADDRESS_REQUIRED, Compatibility.BACKWARD);

            assertTrue(errors.isEmpty(), 
                "Removing streetNumber from required should pass BACKWARD compatibility: " + errors);
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("NONE compatibility should always pass")
        void noneCompatibility_shouldAlwaysPass() {
            String v1 = "{\"type\":\"object\",\"required\":[\"a\",\"b\",\"c\"]}";
            String v2 = "{\"type\":\"object\",\"required\":[]}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.NONE);

            assertTrue(errors.isEmpty());
        }

        @Test
        @DisplayName("Wrapped schema should be handled")
        void wrappedSchema_shouldBeHandled() {
            String v1 = "{\"MyType\":{\"type\":\"object\",\"required\":[\"name\"]}}";
            String v2 = "{\"MyType\":{\"type\":\"object\",\"required\":[]}}";

            List<String> errors = checker.checkCompatibility(v2, v1, Compatibility.FORWARD);

            assertFalse(errors.isEmpty(), "Should detect incompatibility in wrapped schema");
        }

        @Test
        @DisplayName("Invalid JSON should return error")
        void invalidJson_shouldReturnError() {
            List<String> errors = checker.checkCompatibility("not json", "{}", Compatibility.FORWARD);

            assertFalse(errors.isEmpty());
            assertTrue(errors.get(0).contains("parse"));
        }
    }
}
