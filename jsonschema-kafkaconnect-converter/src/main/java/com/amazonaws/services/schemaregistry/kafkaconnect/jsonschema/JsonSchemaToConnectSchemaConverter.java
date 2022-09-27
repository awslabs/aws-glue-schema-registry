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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.ReferenceSchema;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Utilities for mapping between JSON Schema to Connect Schema.
 */
@Data
@Slf4j
public class JsonSchemaToConnectSchemaConverter {
    private Cache<org.everit.json.schema.Schema, Schema> toConnectSchemaCache;
    private boolean connectMetaData;
    private JsonSchemaDataConfig jsonSchemaDataConfig;
    private TypeConverterFactory typeConverterFactory = new TypeConverterFactory();

    public JsonSchemaToConnectSchemaConverter(JsonSchemaDataConfig jsonSchemaDataConfig) {
        this.toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<>(jsonSchemaDataConfig.getSchemasCacheSize()));
        this.connectMetaData = jsonSchemaDataConfig.isConnectMetaData();
        this.jsonSchemaDataConfig = jsonSchemaDataConfig;
    }

    /**
     * Convert the given JsonSchema into a Connect Schema object.
     *
     * @param jsonSchema the JSON schema
     * @return the Connect schema
     */
    public Schema toConnectSchema(org.everit.json.schema.Schema jsonSchema) {
        return toConnectSchema(jsonSchema, true);
    }

    public Schema toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                  Boolean required) {
        if (jsonSchema == null || NullSchema.INSTANCE.equals(jsonSchema)) {
            return null;
        }

        Schema cached = toConnectSchemaCache.get(jsonSchema);
        if (cached != null) {
            return cached;
        }

        final SchemaBuilder builder;
        String connectType = (String) jsonSchema.getUnprocessedProperties()
                .get(JsonSchemaConverterConstants.CONNECT_TYPE_PROP);
        String connectName = (String) jsonSchema.getUnprocessedProperties()
                .get(JsonSchemaConverterConstants.CONNECT_NAME_PROP);

        TypeConverter typeConverter = typeConverterFactory.get(jsonSchema, connectType);

        if (typeConverter != null) {
            builder = typeConverter.toConnectSchema(jsonSchema, jsonSchemaDataConfig);
        } else if (jsonSchema instanceof CombinedSchema) {
            CombinedSchema combinedSchema = (CombinedSchema) jsonSchema;
            Collection<org.everit.json.schema.Schema> subSchemas = combinedSchema.getSubschemas();
            CombinedSchema.ValidationCriterion criterion = combinedSchema.getCriterion();

            boolean hasNullSchema = subSchemas.stream()
                    .anyMatch(schema -> schema instanceof NullSchema);

            boolean isOptionalUnion =
                    CombinedSchema.ONE_CRITERION.equals(criterion) && subSchemas.size() == 2 && hasNullSchema;
            if (isOptionalUnion) {
                return buildOptionalUnionSchema(subSchemas);
            }

            builder = buildNonOptionalUnionSchema(subSchemas, hasNullSchema);
        } else if (jsonSchema instanceof ReferenceSchema) {
            ReferenceSchema refSchema = (ReferenceSchema) jsonSchema;
            return toConnectSchema(refSchema.getReferredSchema(), required);
        } else {
            throw new DataException("Unsupported schema type " + jsonSchema.getClass()
                    .getName());
        }

        populateConnectProperties(builder, jsonSchema, required, connectName);

        Schema result = builder.build();
        toConnectSchemaCache.put(jsonSchema, result);
        return result;
    }

    private Schema buildOptionalUnionSchema(Collection<org.everit.json.schema.Schema> subSchemas) {
        Optional<org.everit.json.schema.Schema> oneOfSchema = subSchemas.stream()
                .filter(schema -> !(schema instanceof NullSchema))
                .findAny();

        if (oneOfSchema.get() instanceof CombinedSchema) {
            CombinedSchema x = (CombinedSchema) oneOfSchema.get();
            Object[] subs = x.getSubschemas().toArray();
            if (subs[0] instanceof org.everit.json.schema.EnumSchema) {
                if (subs[1] instanceof org.everit.json.schema.StringSchema) {
                    return toConnectSchema((org.everit.json.schema.Schema) subs[0], false);
                }
            }
        }
        return toConnectSchema(oneOfSchema.get(), false);
    }

    private SchemaBuilder buildNonOptionalUnionSchema(Collection<org.everit.json.schema.Schema> subSchemas,
                                                      boolean hasNullSchema) {
        SchemaBuilder builder = SchemaBuilder.struct()
                .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF);

        if (hasNullSchema) {
            builder.optional();
        }

        List<org.everit.json.schema.Schema> nonNullSubSchemas = subSchemas.stream()
                .filter(schema -> !(schema instanceof NullSchema))
                .collect(Collectors.toList());

        for (int i = 0; i < nonNullSubSchemas.size(); i++) {
            builder.field("field" + (i + 1), toConnectSchema(nonNullSubSchemas.get(i)));
        }

        return builder;
    }

    private void populateConnectProperties(SchemaBuilder builder,
                                           org.everit.json.schema.Schema jsonSchema,
                                           boolean required,
                                           String connectName) {
        if (required) {
            builder.required();
        } else {
            builder.optional();
        }

        if (connectName != null) {
            builder.name(connectName);
        }

        String connectDoc = (String) jsonSchema.getUnprocessedProperties()
                .get(JsonSchemaConverterConstants.CONNECT_DOC_PROP);
        if (connectDoc != null) {
            builder.doc(connectDoc);
        }

        if (jsonSchema.hasDefaultValue()) {
            builder.defaultValue(jsonSchema.getDefaultValue());
        }

        Integer version = (Integer) jsonSchema.getUnprocessedProperties()
                .get(JsonSchemaConverterConstants.CONNECT_VERSION_PROP);
        if (version != null) {
            builder.version(version);
        }

        Map<String, String> parameters = (Map<String, String>) jsonSchema.getUnprocessedProperties()
                .get(JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP);
        if (parameters != null) {
            builder.parameters(parameters);
        }
    }
}
