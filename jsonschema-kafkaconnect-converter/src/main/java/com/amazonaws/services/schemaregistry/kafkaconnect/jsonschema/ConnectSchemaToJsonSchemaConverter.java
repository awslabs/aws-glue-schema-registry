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

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters.TypeConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters.TypeConverterFactory;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Schema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for mapping between our runtime Connect Schema to JSON Schema.
 */
@Data
@Slf4j
public class ConnectSchemaToJsonSchemaConverter {
    private ConnectValueToJsonNodeConverter connectValueToJsonNodeConverter;
    private TypeConverterFactory typeConverterFactory = new TypeConverterFactory();

    private Cache<Schema, org.everit.json.schema.Schema> fromConnectSchemaCache;
    private boolean connectMetaData;
    private JsonSchemaDataConfig jsonSchemaDataConfig;

    public ConnectSchemaToJsonSchemaConverter(JsonSchemaDataConfig jsonSchemaDataConfig) {
        this.fromConnectSchemaCache =
                new SynchronizedCache<>(new LRUCache<>(jsonSchemaDataConfig.getSchemasCacheSize()));
        this.connectMetaData = jsonSchemaDataConfig.isConnectMetaData();
        this.jsonSchemaDataConfig = jsonSchemaDataConfig;
        this.connectValueToJsonNodeConverter = new ConnectValueToJsonNodeConverter(jsonSchemaDataConfig);
    }

    private static Map<String, String> parametersFromConnect(Map<String, String> params) {
        if (params == null) {
            return null;
        }

        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (!entry.getKey()
                    .equals(JsonSchemaConverterConstants.JSON_FIELD_DEFAULT_FLAG_PROP) && !entry.getKey()
                    .startsWith(JsonSchemaConverterConstants.NAMESPACE)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Converts Connect Schema to Json Schema
     *
     * @param schema Connect Schema
     * @return JSON Schema object
     */
    public org.everit.json.schema.Schema fromConnectSchema(Schema schema) {
        return fromConnectSchema(schema, false);
    }

    public org.everit.json.schema.Schema fromConnectSchema(Schema schema,
                                                           boolean ignoreOptional) {
        return fromConnectSchema(schema, ignoreOptional, -1);
    }

    public org.everit.json.schema.Schema fromConnectSchema(Schema schema,
                                                           boolean ignoreOptional,
                                                           int index) {
        if (schema == null) {
            return NullSchema.INSTANCE;
        }

        org.everit.json.schema.Schema cached = fromConnectSchemaCache.get(schema);

        if (cached != null) {
            return cached;
        }

        Map<String, Object> unprocessedProperties = new HashMap<>();

        TypeConverter typeConverter = typeConverterFactory.get(schema);

        org.everit.json.schema.Schema.Builder baseSchemaBuilder =
                typeConverter.toJsonSchema(schema, unprocessedProperties, jsonSchemaDataConfig);

        // Handles the ordering indicator index so that ordering of Connect Schema is preserved
        if (index != -1) {
            unprocessedProperties.put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, index);
        }

        org.everit.json.schema.Schema finalSchema = baseSchemaBuilder.unprocessedProperties(unprocessedProperties)
                .build();

        if (!(baseSchemaBuilder instanceof CombinedSchema.Builder)) {
            finalSchema =
                    processNonUnionSchema(baseSchemaBuilder, schema, ignoreOptional, unprocessedProperties, index);
        }

        fromConnectSchemaCache.put(schema, finalSchema);
        return finalSchema;
    }

    private org.everit.json.schema.Schema processNonUnionSchema(final org.everit.json.schema.Schema.Builder baseSchemaBuilder,
                                                                Schema schema,
                                                                boolean ignoreOptional,
                                                                Map<String, Object> unprocessedProperties,
                                                                int index) {
        if (connectMetaData) {
            populateConnectMetadata(baseSchemaBuilder, schema, unprocessedProperties);
        }

        org.everit.json.schema.Schema finalSchema;

        boolean shouldBuildOptionalSchema = !ignoreOptional && schema.isOptional();

        if (shouldBuildOptionalSchema) {
            finalSchema = buildOptionalSchema(baseSchemaBuilder, unprocessedProperties, index);
        } else {
            finalSchema = baseSchemaBuilder.unprocessedProperties(unprocessedProperties)
                    .build();
        }

        return finalSchema;
    }

    private void populateConnectMetadata(org.everit.json.schema.Schema.Builder baseSchemaBuilder,
                                         Schema schema,
                                         Map<String, Object> unprocessedProperties) {
        addNonEmptyProperties(unprocessedProperties, JsonSchemaConverterConstants.CONNECT_DOC_PROP, schema.doc());
        addNonEmptyProperties(unprocessedProperties, JsonSchemaConverterConstants.CONNECT_VERSION_PROP,
                              schema.version());
        addNonEmptyProperties(unprocessedProperties, JsonSchemaConverterConstants.CONNECT_NAME_PROP, schema.name());

        Map<String, String> params = parametersFromConnect(schema.parameters());
        if (!MapUtils.isEmpty(params)) {
            unprocessedProperties.put(JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP, params);
        }

        if (schema.defaultValue() != null) {
            addDefaultValue(baseSchemaBuilder, schema);
        }
    }

    private org.everit.json.schema.Schema buildOptionalSchema(final org.everit.json.schema.Schema.Builder baseSchemaBuilder,
                                                              final Map<String, Object> unprocessedProperties,
                                                              final int index) {
        org.everit.json.schema.Schema.Builder combinedSchemaBuilder = CombinedSchema.builder()
                .subschema(baseSchemaBuilder.unprocessedProperties(unprocessedProperties)
                                   .build())
                .subschema(NullSchema.builder()
                                   .build())
                .criterion(CombinedSchema.ONE_CRITERION);

        if (index != -1) {
            combinedSchemaBuilder.unprocessedProperties(
                    Collections.singletonMap(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, index));
        }

        return combinedSchemaBuilder.build();
    }

    private void addNonEmptyProperties(final Map<String, Object> unprocessedProperties,
                                       final String key,
                                       final Object value) {
        if (value != null) {
            unprocessedProperties.put(key, value);
        }
    }

    private void addDefaultValue(final org.everit.json.schema.Schema.Builder baseSchemaBuilder,
                                 final Schema schema) {
        //Bytes schema arrives for Decimal Connect Types and logical types come with schema name property
        //So for none of those set the default value as primitive
        boolean isDefaultValuePrimitive = schema.name() == null && !Schema.BYTES_SCHEMA.type()
                .equals(schema.type());

        boolean hasJsonDefaultFlagProperty = schema.parameters() != null && schema.parameters()
                .containsKey(JsonSchemaConverterConstants.JSON_FIELD_DEFAULT_FLAG_PROP);

        if (!hasJsonDefaultFlagProperty) {
            if (isDefaultValuePrimitive) {
                baseSchemaBuilder.defaultValue(schema.defaultValue());
            } else {
                baseSchemaBuilder.defaultValue(defaultValueFromConnect(schema, schema.defaultValue()));
            }
        }
    }

    private JsonNode defaultValueFromConnect(final Schema schema,
                                             final Object value) {
        return connectValueToJsonNodeConverter.convertToJson(schema, value);
    }
}
