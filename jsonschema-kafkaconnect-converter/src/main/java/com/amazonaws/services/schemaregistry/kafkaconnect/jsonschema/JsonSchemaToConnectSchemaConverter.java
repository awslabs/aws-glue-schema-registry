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

            // A nullable union is a oneOf/anyOf that includes a NullSchema, e.g.
            // {"type": ["string", "null"]} or {"type": ["string", "integer", "null"]}.
            // (A "type" array is parsed by everit as anyOf; an explicit oneOf uses
            // ONE_CRITERION.) In all these cases the null branch means the field is
            // optional, and the field should carry the union of the remaining, non-null
            // types. Build that optional schema and return it directly - it must not fall
            // through to populateConnectProperties, which would call required() on an
            // already-optional builder and throw "optional has already been set".
            // See https://github.com/awslabs/aws-glue-schema-registry/issues/218
            boolean isNullableUnion = hasNullSchema
                    && (CombinedSchema.ONE_CRITERION.equals(criterion)
                            || CombinedSchema.ANY_CRITERION.equals(criterion));
            if (isNullableUnion) {
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
        List<org.everit.json.schema.Schema> nonNullSubSchemas = subSchemas.stream()
                .filter(schema -> !(schema instanceof NullSchema))
                .collect(Collectors.toList());

        // Exactly one real type plus null: the field is simply that type, made optional
        // (e.g. ["string", "null"] -> optional STRING).
        if (nonNullSubSchemas.size() == 1) {
            return toConnectSchema(nonNullSubSchemas.get(0), false);
        }

        // More than one real type plus null (e.g. ["string", "integer", "null"]): build the
        // oneOf-style union struct over the non-null types and mark it optional. Passing
        // hasNullSchema=true makes buildNonOptionalUnionSchema apply optional(); we then
        // build and return it directly rather than routing through populateConnectProperties
        // (which would call required() and throw "optional has already been set").
        SchemaBuilder builder = buildNonOptionalUnionSchema(nonNullSubSchemas, true);
        return builder.build();
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
