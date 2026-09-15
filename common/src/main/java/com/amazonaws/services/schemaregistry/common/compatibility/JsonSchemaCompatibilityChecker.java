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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.glue.model.Compatibility;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Client-side JSON Schema compatibility checker.
 * 
 * This provides compatibility validation that AWS Glue service should perform but doesn't
 * correctly implement for JSON Schema (as of Dec 2024).
 * 
 * Compatibility modes:
 * - BACKWARD: New schema can read old data (new consumers, old producers)
 * - FORWARD: Old schema can read new data (old consumers, new producers)  
 * - FULL: Both backward and forward compatible
 */
public class JsonSchemaCompatibilityChecker {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Check if newSchema is compatible with previousSchema according to the compatibility mode.
     *
     * @param newSchemaDefinition The new schema definition (JSON string)
     * @param previousSchemaDefinition The previous schema definition (JSON string)
     * @param compatibility The compatibility mode to check
     * @return List of compatibility errors (empty if compatible)
     */
    public List<String> checkCompatibility(String newSchemaDefinition,
                                           String previousSchemaDefinition,
                                           Compatibility compatibility) {
        List<String> errors = new ArrayList<>();

        if (compatibility == null || compatibility == Compatibility.NONE 
                || compatibility == Compatibility.UNKNOWN_TO_SDK_VERSION) {
            return errors;
        }

        try {
            JsonNode newSchema = MAPPER.readTree(newSchemaDefinition);
            JsonNode previousSchema = MAPPER.readTree(previousSchemaDefinition);

            String compatStr = compatibility.toString();
            boolean checkBackward = compatStr.startsWith("BACKWARD") || compatStr.startsWith("FULL");
            boolean checkForward = compatStr.startsWith("FORWARD") || compatStr.startsWith("FULL");

            if (checkBackward) {
                errors.addAll(checkBackwardCompatibility(newSchema, previousSchema, ""));
            }
            if (checkForward) {
                errors.addAll(checkForwardCompatibility(newSchema, previousSchema, ""));
            }
        } catch (Exception e) {
            errors.add("Failed to parse schema: " + e.getMessage());
        }

        return errors;
    }

    /**
     * BACKWARD: New schema can read old data.
     * - Cannot add required fields (old data won't have them)
     * - Can remove required fields (making them optional is fine)
     * - Can add optional fields
     */
    private List<String> checkBackwardCompatibility(JsonNode newSchema, JsonNode previousSchema, String path) {
        List<String> errors = new ArrayList<>();

        Set<String> newRequired = getRequiredFields(newSchema);
        Set<String> previousRequired = getRequiredFields(previousSchema);

        // Check for new required fields that weren't required before
        for (String field : newRequired) {
            if (!previousRequired.contains(field)) {
                errors.add(String.format(
                    "BACKWARD incompatible: Field '%s%s' is now required but was not required in previous schema. " +
                    "Old data may not have this field.", 
                    path.isEmpty() ? "" : path + ".", field));
            }
        }

        // Check nested definitions
        errors.addAll(checkDefinitionsCompatibility(newSchema, previousSchema, true));

        return errors;
    }

    /**
     * FORWARD: Old schema can read new data.
     * - Cannot make required fields optional (old consumers expect them)
     * - Cannot remove required fields (old consumers expect them)
     * - Can add new required fields (old consumers ignore extra fields)
     */
    private List<String> checkForwardCompatibility(JsonNode newSchema, JsonNode previousSchema, String path) {
        List<String> errors = new ArrayList<>();

        Set<String> newRequired = getRequiredFields(newSchema);
        Set<String> previousRequired = getRequiredFields(previousSchema);

        // Check for required fields that became optional or were removed
        for (String field : previousRequired) {
            if (!newRequired.contains(field)) {
                errors.add(String.format(
                    "FORWARD incompatible: Field '%s%s' was required but is now optional or removed. " +
                    "Old consumers expect this field to be present.",
                    path.isEmpty() ? "" : path + ".", field));
            }
        }

        // Check nested definitions
        errors.addAll(checkDefinitionsCompatibility(newSchema, previousSchema, false));

        return errors;
    }

    /**
     * Check compatibility of nested definitions (for $ref support).
     */
    private List<String> checkDefinitionsCompatibility(JsonNode newSchema, JsonNode previousSchema, 
                                                        boolean isBackward) {
        List<String> errors = new ArrayList<>();

        JsonNode newDefs = getDefinitions(newSchema);
        JsonNode prevDefs = getDefinitions(previousSchema);

        if (newDefs == null || prevDefs == null) {
            return errors;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = prevDefs.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String defName = entry.getKey();
            JsonNode prevDef = entry.getValue();
            JsonNode newDef = newDefs.get(defName);

            if (newDef != null) {
                String defPath = "definitions." + defName;
                if (isBackward) {
                    errors.addAll(checkBackwardCompatibility(newDef, prevDef, defPath));
                } else {
                    errors.addAll(checkForwardCompatibility(newDef, prevDef, defPath));
                }
            }
        }

        return errors;
    }

    /**
     * Extract required fields from a JSON Schema node.
     */
    private Set<String> getRequiredFields(JsonNode schema) {
        Set<String> required = new HashSet<>();

        // Handle wrapper object (e.g., {"MyType": {...schema...}})
        JsonNode schemaNode = unwrapSchema(schema);

        JsonNode requiredNode = schemaNode.get("required");
        if (requiredNode != null && requiredNode.isArray()) {
            for (JsonNode field : requiredNode) {
                required.add(field.asText());
            }
        }

        return required;
    }

    /**
     * Get definitions node (supports both "definitions" and "$defs").
     */
    private JsonNode getDefinitions(JsonNode schema) {
        JsonNode schemaNode = unwrapSchema(schema);
        
        JsonNode defs = schemaNode.get("definitions");
        if (defs != null) {
            return defs;
        }
        return schemaNode.get("$defs");
    }

    /**
     * Unwrap schema if it's wrapped in a named object.
     * Handles schemas like: {"MyTypeName": {"type": "object", ...}}
     */
    private JsonNode unwrapSchema(JsonNode schema) {
        if (schema.has("type") || schema.has("$schema") || schema.has("properties")) {
            return schema;
        }

        // Check if it's a wrapper with a single named schema
        Iterator<Map.Entry<String, JsonNode>> fields = schema.fields();
        JsonNode firstValue = null;
        int count = 0;
        
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            // Skip known non-wrapper keys
            if (key.equals("definitions") || key.equals("$defs")) {
                continue;
            }
            firstValue = entry.getValue();
            count++;
        }

        // If there's exactly one non-definition field and it looks like a schema, unwrap it
        if (count == 1 && firstValue != null && 
            (firstValue.has("type") || firstValue.has("properties"))) {
            return firstValue;
        }

        return schema;
    }
}
