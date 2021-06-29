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

/**
 * Glue Schema Registry JSON Schema converter constants.
 */
public class JsonSchemaConverterConstants {
    public static final String NAMESPACE = "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema";
    public static final String JSON_SCHEMA_TYPE_ENUM = NAMESPACE + ".Enum";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String JSON_FIELD_DEFAULT_FLAG_PROP = NAMESPACE + ".field.default";
    public static final String CONNECT_NAME_PROP = "connect.name";
    public static final String CONNECT_DOC_PROP = "connect.doc";
    public static final String CONNECT_VERSION_PROP = "connect.version";
    public static final String CONNECT_PARAMETERS_PROP = "connect.parameters";
    public static final String CONNECT_TYPE_PROP = "connect.type";
    public static final String CONNECT_INDEX_PROP = "connect.index";
    public static final String JSON_SCHEMA_TYPE_ONEOF = "oneOf";
}
