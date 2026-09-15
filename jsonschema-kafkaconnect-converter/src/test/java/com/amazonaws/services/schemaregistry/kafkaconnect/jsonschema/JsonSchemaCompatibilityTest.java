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

import com.amazonaws.services.schemaregistry.common.compatibility.JsonSchemaCompatibilityChecker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Compatibility;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests demonstrating that the new JsonSchemaCompatibilityChecker correctly catches
 * the AWS Glue service bug where FORWARD compatibility is not enforced for JSON Schema.
 */
public class JsonSchemaCompatibilityTest {

    private JsonSchemaCompatibilityChecker checker;

    @BeforeEach
    void setUp() {
        checker = new JsonSchemaCompatibilityChecker();
    }

    @Nested
    @DisplayName("Real-World Bug: Employee Schema streetNumber")
    class RealWorldBugTests {

        // Simplified version of the actual customer schema - V1 with streetNumber required
        private static final String V1_ADDRESS = 
            "{\"definitions\":{\"Address\":{\"type\":\"object\"," +
            "\"properties\":{\"streetNumber\":{\"type\":\"number\"},\"city\":{\"type\":\"string\"}}," +
            "\"required\":[\"streetNumber\",\"city\"]}}}";

        // V2 with streetNumber removed from required (now optional)
        private static final String V2_ADDRESS = 
            "{\"definitions\":{\"Address\":{\"type\":\"object\"," +
            "\"properties\":{\"streetNumber\":{\"type\":\"number\"},\"city\":{\"type\":\"string\"}}," +
            "\"required\":[\"city\"]}}}";

        @Test
        @DisplayName("Client-side checker catches the bug AWS Glue misses")
        void clientSideChecker_catchesBug_awsGlueMisses() {
            /*
             * REPRODUCTION OF AWS GLUE BUG:
             * 
             * 1. aws glue create-schema --compatibility FORWARD --schema-definition <V1>
             *    V1 has Address.streetNumber in required array
             * 
             * 2. aws glue register-schema-version --schema-definition <V2>
             *    V2 removes streetNumber from required array
             * 
             * AWS GLUE BEHAVIOR: Accepts V2 (WRONG!)
             * CLIENT CHECKER BEHAVIOR: Rejects V2 (CORRECT!)
             */
            
            List<String> errors = checker.checkCompatibility(V2_ADDRESS, V1_ADDRESS, Compatibility.FORWARD);

            assertFalse(errors.isEmpty(), 
                "Client-side checker correctly catches FORWARD incompatibility that AWS Glue misses");
            assertTrue(errors.get(0).contains("streetNumber"),
                "Error message should identify the problematic field");
            assertTrue(errors.get(0).contains("FORWARD"),
                "Error message should identify the compatibility mode violated");
            
            System.out.println("✓ Client-side checker caught the bug:");
            System.out.println("  " + errors.get(0));
        }

        @Test
        @DisplayName("Same change passes BACKWARD compatibility (as expected)")
        void sameChange_passesBackward() {
            // Making a field optional is fine for BACKWARD compatibility
            // (new consumers can handle old data that has the field)
            
            List<String> errors = checker.checkCompatibility(V2_ADDRESS, V1_ADDRESS, Compatibility.BACKWARD);

            assertTrue(errors.isEmpty(), 
                "Making streetNumber optional should pass BACKWARD compatibility");
        }
    }

    @Nested
    @DisplayName("Compatibility Rules Verification")
    class CompatibilityRulesTests {

        private static final String SCHEMA_V1 = 
            "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";
        
        private static final String SCHEMA_V2_OPTIONAL = 
            "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[]}";

        private static final String SCHEMA_V2_NEW_REQUIRED = 
            "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"}},\"required\":[\"name\",\"email\"]}";

        @Test
        @DisplayName("FORWARD: required→optional FAILS")
        void forward_requiredToOptional_fails() {
            List<String> errors = checker.checkCompatibility(SCHEMA_V2_OPTIONAL, SCHEMA_V1, Compatibility.FORWARD);
            assertFalse(errors.isEmpty());
        }

        @Test
        @DisplayName("FORWARD: adding required field PASSES")
        void forward_addingRequired_passes() {
            List<String> errors = checker.checkCompatibility(SCHEMA_V2_NEW_REQUIRED, SCHEMA_V1, Compatibility.FORWARD);
            assertTrue(errors.isEmpty(), "Adding required field should pass FORWARD: " + errors);
        }

        @Test
        @DisplayName("BACKWARD: required→optional PASSES")
        void backward_requiredToOptional_passes() {
            List<String> errors = checker.checkCompatibility(SCHEMA_V2_OPTIONAL, SCHEMA_V1, Compatibility.BACKWARD);
            assertTrue(errors.isEmpty());
        }

        @Test
        @DisplayName("BACKWARD: adding required field FAILS")
        void backward_addingRequired_fails() {
            List<String> errors = checker.checkCompatibility(SCHEMA_V2_NEW_REQUIRED, SCHEMA_V1, Compatibility.BACKWARD);
            assertFalse(errors.isEmpty());
        }

        @Test
        @DisplayName("FULL: required→optional FAILS (violates FORWARD)")
        void full_requiredToOptional_fails() {
            List<String> errors = checker.checkCompatibility(SCHEMA_V2_OPTIONAL, SCHEMA_V1, Compatibility.FULL);
            assertFalse(errors.isEmpty());
            assertTrue(errors.stream().anyMatch(e -> e.contains("FORWARD")));
        }

        @Test
        @DisplayName("FULL: adding required field FAILS (violates BACKWARD)")
        void full_addingRequired_fails() {
            List<String> errors = checker.checkCompatibility(SCHEMA_V2_NEW_REQUIRED, SCHEMA_V1, Compatibility.FULL);
            assertFalse(errors.isEmpty());
            assertTrue(errors.stream().anyMatch(e -> e.contains("BACKWARD")));
        }
    }
}
