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
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.ReferenceSchema;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
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
        } else if (jsonSchema instanceof ConstSchema) {
            // JSON Schema "const" (draft-06+) restricts a value to a single permitted
            // value, which may be any JSON value - scalar, object, or array. everit exposes
            // that value via getPermittedValue(). We infer a Connect schema from the value's
            // shape so both schema translation and value deserialization work through the
            // normal Connect type machinery. See issue: const types unsupported.
            builder = buildConstSchema((ConstSchema) jsonSchema);
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
        return toConnectSchema(oneOfSchema.get(), false);
    }

    /**
     * Builds a Connect schema for a JSON Schema {@code const} by inferring the schema from
     * the Java type of the single permitted value. Objects become STRUCTs, arrays become
     * ARRAYs (with a homogeneous element type), and scalars map to their Connect types.
     *
     * <p>Note: this preserves the <em>type</em> of the const value so that data flows
     * through correctly, but it does not enforce the const <em>constraint</em> (that the
     * value must equal the permitted value); Connect's schema model has no equivalent, and
     * the constraint is not round-tripped, consistent with how other JSON Schema validation
     * keywords (pattern, minimum, format, ...) are handled.
     */
    private SchemaBuilder buildConstSchema(ConstSchema constSchema) {
        return inferSchemaBuilderFromValue(constSchema.getPermittedValue());
    }

    private SchemaBuilder inferSchemaBuilderFromValue(Object value) {
        if (value == null) {
            throw new DataException(
                    "Cannot infer a Connect schema for a JSON Schema 'const' with a null value");
        } else if (value instanceof Boolean) {
            return SchemaBuilder.bool();
        } else if (value instanceof Integer || value instanceof Long || value instanceof Short
                || value instanceof Byte || value instanceof BigInteger) {
            return SchemaBuilder.int64();
        } else if (value instanceof Number) {
            // Float, Double, BigDecimal and any other non-integral number.
            return SchemaBuilder.float64();
        } else if (value instanceof CharSequence) {
            return SchemaBuilder.string();
        } else if (value instanceof Map) {
            return inferStructBuilderFromValue((Map<?, ?>) value);
        } else if (value instanceof Collection) {
            return inferArrayBuilderFromValue((Collection<?>) value);
        }
        throw new DataException("Unsupported JSON Schema 'const' value type: "
                + value.getClass().getName());
    }

    private SchemaBuilder inferStructBuilderFromValue(Map<?, ?> map) {
        SchemaBuilder builder = SchemaBuilder.struct();
        // Sort by key so the generated STRUCT has a deterministic field order regardless of
        // the Map implementation everit returns.
        Map<String, Object> sortedFields = new TreeMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            sortedFields.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        for (Map.Entry<String, Object> entry : sortedFields.entrySet()) {
            if (entry.getValue() == null) {
                throw new DataException("Cannot infer a Connect schema for the 'const' object field '"
                        + entry.getKey() + "' because its value is null");
            }
            builder.field(entry.getKey(), inferSchemaBuilderFromValue(entry.getValue()).build());
        }
        return builder;
    }

    private SchemaBuilder inferArrayBuilderFromValue(Collection<?> collection) {
        if (collection.isEmpty()) {
            throw new DataException(
                    "Cannot infer a Connect element schema for an empty 'const' array");
        }
        Schema elementSchema = null;
        for (Object element : collection) {
            if (element == null) {
                throw new DataException(
                        "Cannot infer a Connect schema for a null element in a 'const' array");
            }
            Schema candidate = inferSchemaBuilderFromValue(element).build();
            if (elementSchema == null) {
                elementSchema = candidate;
            } else if (!elementSchema.equals(candidate)) {
                throw new DataException("Cannot convert a heterogeneous 'const' array to a Connect ARRAY; "
                        + "all elements must have the same type, but found " + elementSchema.type()
                        + " and " + candidate.type());
            }
        }
        return SchemaBuilder.array(elementSchema);
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
