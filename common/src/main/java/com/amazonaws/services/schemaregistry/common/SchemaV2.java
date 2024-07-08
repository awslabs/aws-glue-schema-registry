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

package com.amazonaws.services.schemaregistry.common;

import lombok.AllArgsConstructor;
import lombok.Value;
import software.amazon.awssdk.services.glue.model.Compatibility;

/**
 * Schema entity represents a schema and it's properties stored in Glue Schema Registry.
 */
@AllArgsConstructor
@Value
public class SchemaV2 {
    /**
     * Schema Definition contains the string representation of schema version stored during registration.
     */
    private String schemaDefinition;

    /**
     * Data Format represents the string notation of data format used during registration of schea. Ex: Avro, JSON etc.
     */
    private String dataFormat;

    /**
     * Schema Name represents name of the schema under which the schema version was registered.
     */
    private String schemaName;

    /**
     * Compatibility mode refers to the compatibility settings of the schema.
     */
    private Compatibility compatibilityMode;
}
