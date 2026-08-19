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
package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters;

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.ConnectSchemaToJsonSchemaConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.ConnectValueToJsonNodeConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonNodeToConnectValueConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaConverterConstants;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaToConnectSchemaConverter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.ObjectSchema;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StructTypeConverter implements TypeConverter {
    // Json Java object types used by Connect schema types
    private static final Map<Schema.Type, List<Class>> SIMPLE_JSON_SCHEMA_TYPES = new HashMap<>();

    static {
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.INT8, Arrays.asList(IntNode.class, LongNode.class, BigIntegerNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.INT16, Arrays.asList(IntNode.class, LongNode.class, BigIntegerNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.INT32, Arrays.asList(IntNode.class, LongNode.class, BigIntegerNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.INT64, Arrays.asList(IntNode.class, LongNode.class,
                                                                      BigIntegerNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.FLOAT32,
                                     Arrays.asList(FloatNode.class, DoubleNode.class, DecimalNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.FLOAT64,
                                     Arrays.asList(FloatNode.class, DoubleNode.class, DecimalNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.BOOLEAN, Arrays.asList((Class) BooleanNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.STRING, Arrays.asList((Class) TextNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.BYTES, Arrays.asList(BinaryNode.class, BigDecimal.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.ARRAY, Arrays.asList((Class) ArrayNode.class));
        SIMPLE_JSON_SCHEMA_TYPES.put(Schema.Type.MAP, Arrays.asList(Map.class, ArrayNode.class));
    }

    private ConnectValueToJsonNodeConverter connectValueToJsonNodeConverter;
    private ConnectSchemaToJsonSchemaConverter connectSchemaToJsonSchemaConverter;
    private JsonNodeToConnectValueConverter jsonNodeToConnectValueConverter;
    private JsonSchemaToConnectSchemaConverter jsonSchemaToConnectSchemaConverter;

    public static Schema nonOptional(Schema schema) {
        return new ConnectSchema(schema.type(), false, schema.defaultValue(), schema.name(), schema.version(),
                                 schema.doc(), schema.parameters(), fields(schema), keySchema(schema),
                                 valueSchema(schema));
    }

    public static List<Field> fields(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.STRUCT.equals(type)) {
            return schema.fields();
        } else {
            return null;
        }
    }

    public static Schema keySchema(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.MAP.equals(type)) {
            return schema.keySchema();
        } else {
            return null;
        }
    }

    public static Schema valueSchema(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.MAP.equals(type) || Schema.Type.ARRAY.equals(type)) {
            return schema.valueSchema();
        } else {
            return null;
        }
    }

    private static boolean isInstanceOfJsonSchemaTypeForSimpleSchema(Schema fieldSchema,
                                                                     JsonNode value) {
        List<Class> classes = SIMPLE_JSON_SCHEMA_TYPES.get(fieldSchema.type());
        if (classes == null) {
            return false;
        }
        for (Class type : classes) {
            if (type.isInstance(value)) {
                if (fieldSchema.type()
                            .equals(Schema.Type.MAP) && value.isArray()) {
                    return isMapAsArray(value);
                }
                return true;
            }
        }
        return false;
    }

    private static boolean structSchemaEquals(Schema fieldSchema,
                                              JsonNode value) {
        if (fieldSchema.type() != Schema.Type.STRUCT || !value.isObject()) {
            return false;
        }
        Set<String> schemaFields = fieldSchema.fields()
                .stream()
                .map(Field::name)
                .collect(Collectors.toSet());

        Iterator<Map.Entry<String, JsonNode>> jsonNodeIterator = value.fields();
        while (jsonNodeIterator.hasNext()) {
            String valueFieldName = jsonNodeIterator.next()
                    .getKey();
            if (schemaFields.contains(valueFieldName)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isMapAsArray(JsonNode value) {
        ArrayNode arrayNode = (ArrayNode) value;
        for (int i = 0; i < arrayNode.size(); i++) {
            JsonNode currentNode = arrayNode.get(i);
            if (!currentNode.has(JsonSchemaConverterConstants.KEY_FIELD) && !currentNode.has(
                    JsonSchemaConverterConstants.VALUE_FIELD)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Connect Struct type is a structured record containing a set of named fields with values, each field using an
     * independent Schema.
     * Struct objects must specify a complete Schema up front, and only fields specified in the Schema may be set.
     * <p>
     * Structs are represented as JSON Object
     * e.g.
     * <p>
     * {
     * "name":john,
     * "age":42
     * }
     *
     * @param schema connectSchema
     * @param value  connectValue
     * @param config jsonSchemaDataConfig
     * @return JsonNode converted json Object
     */
    @Override
    public JsonNode toJson(final Schema schema,
                           final Object value,
                           final JsonSchemaDataConfig config) {
        connectValueToJsonNodeConverter = new ConnectValueToJsonNodeConverter(config);
        Struct struct = (Struct) value;
        if (!struct.schema()
                .equals(schema)) {
            throw new DataException("Mismatching schema.");
        }

        // Handle JSON Union/One Of type Schemas that are not resulted from Optional
        // e.g.
        //  {"oneOf":[{"type":"integer","connect.type":"int32"},{"type":"string"}]}

        if (JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF.equals(schema.name())) {
            for (Field field : schema.fields()) {
                Object obj = struct.get(field);
                if (obj != null) {
                    return connectValueToJsonNodeConverter.convertToJson(field.schema(), obj);
                }
            }
            return connectValueToJsonNodeConverter.convertToJson(schema, null);
        } else {
            ObjectNode obj = JSON_NODE_FACTORY.objectNode();
            for (Field field : schema.fields()) {
                if (struct.get(field) != null) {
                    obj.set(field.name(),
                            connectValueToJsonNodeConverter.convertToJson(field.schema(), struct.get(field)));
                }
            }
            return obj;
        }
    }

    @Override
    public org.everit.json.schema.Schema.Builder toJsonSchema(final Schema schema,
                                                              final Map<String, Object> unprocessedProperties,
                                                              final JsonSchemaDataConfig jsonSchemaDataConfig) {
        connectSchemaToJsonSchemaConverter = new ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig);
        org.everit.json.schema.Schema.Builder baseSchemaBuilder;
        if (JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF.equals(schema.name())) {
            Collection<org.everit.json.schema.Schema> unionSchemas = new ArrayList();
            if (schema.isOptional()) {
                unionSchemas.add(NullSchema.builder()
                                         .build());
            }
            for (Field field : schema.fields()) {
                unionSchemas.add(connectSchemaToJsonSchemaConverter.fromConnectSchema(nonOptional(field.schema()), true,
                                                                                      field.index()));
            }
            baseSchemaBuilder = CombinedSchema.oneOf(unionSchemas);
        } else if (schema.isOptional()) {
            Collection<org.everit.json.schema.Schema> unionSchemas = new ArrayList<>();
            unionSchemas.add(NullSchema.builder()
                                     .build());
            unionSchemas.add(connectSchemaToJsonSchemaConverter.fromConnectSchema(nonOptional(schema), false));
            baseSchemaBuilder = CombinedSchema.oneOf(unionSchemas);

        } else {
            ObjectSchema.Builder objectSchemaBuilder = ObjectSchema.builder();
            for (Field field : schema.fields()) {
                objectSchemaBuilder.addPropertySchema(field.name(),
                                                      connectSchemaToJsonSchemaConverter.fromConnectSchema(
                                                              field.schema(), false, field.index()));
            }
            baseSchemaBuilder = objectSchemaBuilder;
        }
        return baseSchemaBuilder;
    }

    @Override
    public Object toConnect(final Schema schema,
                            final JsonNode value,
                            final JsonSchemaDataConfig jsonSchemaDataConfig) {
        jsonNodeToConnectValueConverter = new JsonNodeToConnectValueConverter(jsonSchemaDataConfig);
        Struct converted = null;
        if (schema.name() != null && schema.name()
                .equals(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)) {
            // Special case support for union types
            for (Field field : schema.fields()) {
                Schema fieldSchema = field.schema();

                if (isInstanceOfJsonSchemaTypeForSimpleSchema(fieldSchema, value) || structSchemaEquals(fieldSchema,
                                                                                                          value)) {
                    if (converted == null) {
                        converted = new Struct(schema.schema());
                    }
                    converted.put("field" + (field.index() + 1),
                            jsonNodeToConnectValueConverter.toConnectValue(
                                    fieldSchema, value));

                }
            }
            if (converted == null) {
                throw new DataException("Did not find matching union field for data: " + value.toString());
            }
        } else {
            if (!value.isObject()) {
                throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());
            }

            // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
            // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
            // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
            // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema
            // .schema()
            // just returns the schema Object and has no overhead.
            Struct result = new Struct(schema.schema());
            for (Field field : schema.fields()) {
                Object fieldValue = value.get(field.name());
                if (fieldValue != null) {
                    result.put(field,
                               jsonNodeToConnectValueConverter.toConnectValue(field.schema(), value.get(field.name())));
                }
            }

            return result;
        }
        return converted;
    }

    @Override
    public SchemaBuilder toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                         JsonSchemaDataConfig jsonSchemaDataConfig) {
        jsonSchemaToConnectSchemaConverter = new JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig);
        SchemaBuilder builder = SchemaBuilder.struct();

        if (jsonSchema instanceof ObjectSchema) {

            ObjectSchema objectSchema = (ObjectSchema) jsonSchema;
            LinkedHashMap<String, org.everit.json.schema.Schema> sortedPropertiesByIndex =
                    getOrderedFields(objectSchema);

            for (Map.Entry<String, org.everit.json.schema.Schema> property : sortedPropertiesByIndex.entrySet()) {
                String subFieldName = property.getKey();
                org.everit.json.schema.Schema subSchema = property.getValue();
                builder.field(subFieldName, jsonSchemaToConnectSchemaConverter.toConnectSchema(subSchema, true));
            }

            return builder;
        } else {
            throw new DataException("Non Object Json Schema can not be converted to Connect Struct type Schema.");
        }
    }

    private LinkedHashMap<String, org.everit.json.schema.Schema> getOrderedFields(ObjectSchema objectSchema) {
        Map<String, org.everit.json.schema.Schema> properties = objectSchema.getPropertySchemas();

        Comparator<Map.Entry<String, org.everit.json.schema.Schema>> jsonSchemaComparator = (s1, s2) -> {
            Integer index1 = (Integer) s1.getValue()
                    .getUnprocessedProperties()
                    .get(JsonSchemaConverterConstants.CONNECT_INDEX_PROP);
            Integer index2 = (Integer) s2.getValue()
                    .getUnprocessedProperties()
                    .get(JsonSchemaConverterConstants.CONNECT_INDEX_PROP);

            // Additional comparison logic in case schemas are not enriched with connect.index property
            // This case occurs when Schemas are not produced using a Source side converter. i.e. a plain Producer submits data to kafka topic
            // and registers the schema with Glue Schema Registry.
            // Producer -> Kafka Topic -> Sink Connector -> Destination
            //       |                     ^
            //       v                     |
            //       AWS Glue Schema Registry

            if (index1 == null && index2 == null) {
                return 0;
            }
            if (index1 == null) return 1;
            if (index2 == null) return -1;

            return index1.compareTo(index2);
        };

        return properties.entrySet()
                .stream()
                .sorted(jsonSchemaComparator)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }
}
