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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.junit.jupiter.params.provider.Arguments;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

public class TestDataProvider {
    static final JsonNodeFactory JSON_NODE_FACTORY = TypeConverter.JSON_NODE_FACTORY;

    static final NullNode NULL_NODE = NullNode.getInstance();
    static final org.everit.json.schema.Schema NULL_SCHEMA = NullSchema.INSTANCE;
    static final org.everit.json.schema.Schema BOOLEAN_SCHEMA = BooleanSchema.builder()
            .build();
    static final org.everit.json.schema.Schema.Builder BYTE_SCHEMA_BUILDER = NumberSchema.builder()
            .requiresInteger(true)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int8");
            }});
    static final org.everit.json.schema.Schema BYTE_SCHEMA = BYTE_SCHEMA_BUILDER.build();
    static final org.everit.json.schema.Schema.Builder SHORT_SCHEMA_BUILDER = NumberSchema.builder()
            .requiresInteger(true)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int16");
            }});
    static final org.everit.json.schema.Schema SHORT_SCHEMA = SHORT_SCHEMA_BUILDER.build();
    static final org.everit.json.schema.Schema.Builder INT_SCHEMA_BUILDER = NumberSchema.builder()
            .requiresInteger(true)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int32");
            }});
    static final org.everit.json.schema.Schema INT_SCHEMA = INT_SCHEMA_BUILDER.build();
    static final org.everit.json.schema.Schema.Builder LONG_SCHEMA_BUILDER = NumberSchema.builder()
            .requiresInteger(true)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int64");
            }});
    static final org.everit.json.schema.Schema LONG_SCHEMA = LONG_SCHEMA_BUILDER.build();
    static final org.everit.json.schema.Schema.Builder FLOAT_SCHEMA_BUILDER = NumberSchema.builder()
            .requiresInteger(false)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "float32");
            }});
    static final org.everit.json.schema.Schema FLOAT_SCHEMA = FLOAT_SCHEMA_BUILDER.build();
    static final org.everit.json.schema.Schema.Builder DOUBLE_SCHEMA_BUILDER = NumberSchema.builder()
            .requiresInteger(false)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "float64");
            }});
    static final org.everit.json.schema.Schema DOUBLE_SCHEMA = DOUBLE_SCHEMA_BUILDER.build();
    static final org.everit.json.schema.Schema.Builder BYTES_SCHEMA_BUILDER = StringSchema.builder()
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "bytes");
            }});
    static final org.everit.json.schema.Schema BYTES_SCHEMA = BYTES_SCHEMA_BUILDER.build();
    static final org.everit.json.schema.Schema STRING_SCHEMA = StringSchema.builder()
            .build();
    static final org.everit.json.schema.Schema ENUM_SCHEMA = EnumSchema.builder()
            .possibleValues(Sets.newHashSet("apple", "banana", "mango"))
            .build();
    static final Schema CONNECT_ENUM_SCHEMA = new SchemaBuilder(Schema.Type.STRING).
            parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM, null)
            .parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + ".apple", "apple")
            .parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + ".banana", "banana")
            .parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + ".mango", "mango")
            .build();
    static final org.everit.json.schema.Schema.Builder MAP_SCHEMA_WITH_STRING_KEY_BUILDER = ObjectSchema.builder()
            .schemaOfAdditionalProperties(INT_SCHEMA)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "map");
            }});
    static final org.everit.json.schema.Schema MAP_SCHEMA_WITH_STRING_KEY = MAP_SCHEMA_WITH_STRING_KEY_BUILDER.build();
    static final Schema CONNECT_MAP_SCHEMA_WITH_STRING_KEY =
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
                    .build();
    static final Schema CONNECT_MAP_SCHEMA_WITH_INTEGER_KEY =
            SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
                    .build();
    static final org.everit.json.schema.Schema.Builder ARRAY_SCHEMA_BUILDER = ArraySchema.builder()
            .allItemSchema(STRING_SCHEMA);
    static final org.everit.json.schema.Schema ARRAY_SCHEMA = ARRAY_SCHEMA_BUILDER.build();
    static final org.everit.json.schema.Schema OPTIONAL_BOOLEAN_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(BOOLEAN_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT =
            CombinedSchema.oneOf(Arrays.asList(BOOLEAN_SCHEMA, NULL_SCHEMA))
                    .defaultValue(null)
                    .build();
    static final Schema CONNECT_OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT = SchemaBuilder.bool()
            .optional()
            .defaultValue(null)
            .build();
    static final org.everit.json.schema.Schema OPTIONAL_BYTE_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(BYTE_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema OPTIONAL_SHORT_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(SHORT_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema OPTIONAL_INT_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(INT_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema OPTIONAL_LONG_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(LONG_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema OPTIONAL_FLOAT_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(FLOAT_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema OPTIONAL_DOUBLE_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(DOUBLE_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema OPTIONAL_BYTES_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(BYTES_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema OPTIONAL_STRING_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(STRING_SCHEMA, NULL_SCHEMA))
                    .build();
    static final org.everit.json.schema.Schema MAP_SCHEMA_WITH_OPTIONAL_STRING_KEY = ObjectSchema.builder()
            .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD, OPTIONAL_STRING_SCHEMA)
            .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD, INT_SCHEMA)
            .build();
    static final org.everit.json.schema.Schema MAP_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY = ArraySchema.builder()
            .allItemSchema(MAP_SCHEMA_WITH_OPTIONAL_STRING_KEY)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "map");
            }})
            .build();
    static final JsonNode MAP_JSON_DATA_WITH_STRING_KEY = JSON_NODE_FACTORY.objectNode()
            .put(JsonSchemaConverterConstants.KEY_FIELD, "answer")
            .put(JsonSchemaConverterConstants.VALUE_FIELD, 42);
    static final ArrayNode MAP_JSON_DATA_AS_ARRAY_WITH_STRING_KEY = JSON_NODE_FACTORY.arrayNode()
            .add(MAP_JSON_DATA_WITH_STRING_KEY);
    static final JsonNode MAP_JSON_DATA_WITH_NULL_KEY = JSON_NODE_FACTORY.objectNode()
            .put(JsonSchemaConverterConstants.KEY_FIELD, (String) null)
            .put(JsonSchemaConverterConstants.VALUE_FIELD, 42);
    static final ArrayNode MAP_JSON_DATA_AS_ARRAY_WITH_NULL_KEY = JSON_NODE_FACTORY.arrayNode()
            .add(MAP_JSON_DATA_WITH_NULL_KEY);
    static final org.everit.json.schema.Schema MAP_SCHEMA_WITH_INTEGER_KEY = ObjectSchema.builder()
            .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD, INT_SCHEMA)
            .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD, INT_SCHEMA)
            .build();
    static final org.everit.json.schema.Schema.Builder MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY_BUILDER = ArraySchema.builder()
            .allItemSchema(MAP_SCHEMA_WITH_INTEGER_KEY)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "map");
            }});
    static final org.everit.json.schema.Schema MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY =
            MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY_BUILDER.build();

    static final JsonNode MAP_JSON_DATA_WITH_INTEGER_KEY = JSON_NODE_FACTORY.objectNode()
            .put(JsonSchemaConverterConstants.KEY_FIELD, 42)
            .put(JsonSchemaConverterConstants.VALUE_FIELD, 42);
    static final ArrayNode MAP_JSON_DATA_AS_ARRAY_WITH_INTEGER_KEY = JsonNodeFactory.instance.arrayNode()
            .add(MAP_JSON_DATA_WITH_INTEGER_KEY);
    static final Schema CONNECT_NAMED_MAP_SCHEMA = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
            .name("foo.bar")
            .build();
    static final org.everit.json.schema.Schema MAP_NAMED_SCHEMA_WITH_OPTIONAL_STRING_KEY = ObjectSchema.builder()
            .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD, OPTIONAL_STRING_SCHEMA)
            .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD, INT_SCHEMA)
            .build();
    static final org.everit.json.schema.Schema MAP_NAMED_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY = ArraySchema.builder()
            .allItemSchema(MAP_NAMED_SCHEMA_WITH_OPTIONAL_STRING_KEY)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "map");
                put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, "foo.bar");
            }})
            .build();

    static final Schema CONNECT_STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("int32", Schema.INT32_SCHEMA)
            .build();
    static final Schema CONNECT_STRUCT_STRING_SCHEMA = SchemaBuilder.struct()
            .field("int32", Schema.STRING_SCHEMA)
            .build();
    static final Struct CONNECT_STRUCT_VALUE = new Struct(CONNECT_STRUCT_SCHEMA).put("int32", 42);
    static final org.everit.json.schema.Schema JSON_STRUCT_SCHEMA = ObjectSchema.builder()
            .addPropertySchema("int32", buildSchemaWithIndex(INT_SCHEMA_BUILDER, 0))
            .build();
    static final JsonNode JSON_STRUCT_DATA = JsonNodeFactory.instance.objectNode()
            .put("int32", 42);
    static final Schema CONNECT_OPTIONAL_STRUCT_SCHEMA = SchemaBuilder.struct()
            .optional()
            .field("int32", Schema.INT32_SCHEMA)
            .build();
    static final org.everit.json.schema.Schema JSON_STRUCT_OPTIONAL_SCHEMA =
            CombinedSchema.oneOf(Arrays.asList(NullSchema.INSTANCE, JSON_STRUCT_SCHEMA))
                    .build();
    static final Struct CONNECT_STRUCT_OPTIONAL_VALUE = new Struct(CONNECT_OPTIONAL_STRUCT_SCHEMA).put("int32", 42);

    static final Schema CONNECT_DECIMAL_SCHEMA = Decimal.builder(2)
            .build();
    static final org.everit.json.schema.Schema NUMBER_DECIMAL_SCHEMA = NumberSchema.builder()
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "bytes");
                put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Decimal.LOGICAL_NAME);
                put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                put(JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP, Collections.singletonMap("scale", "2"));
            }})
            .build();
    static final org.everit.json.schema.Schema STRING_DECIMAL_SCHEMA = StringSchema.builder()
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "bytes");
                put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Decimal.LOGICAL_NAME);
                put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                put(JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP, Collections.singletonMap("scale", "2"));
            }})
            .build();
    static final BigDecimal CONNECT_DECIMAL_VALUE = BigDecimal.valueOf(156, 2);
    static final JsonNode NUMERIC_DECIMAL_JSON_NODE = JSON_NODE_FACTORY.numberNode(CONNECT_DECIMAL_VALUE);
    static final JsonNode BASE64_DECIMAL_JSON_NODE = JSON_NODE_FACTORY.binaryNode(new byte[]{0, -100});
    static final BigDecimal CONNECT_DECIMAL_VALUE_TRAILING_ZEROES = new BigDecimal("156.00");
    static final JsonNode NUMERIC_DECIMAL_JSON_NODE_TRAILING_ZEROES =
            JSON_NODE_FACTORY.numberNode(CONNECT_DECIMAL_VALUE_TRAILING_ZEROES);

    static final Schema CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA = Decimal.builder(17)
            .build();
    static final org.everit.json.schema.Schema NUMBER_HIGH_PRECISION_DECIMAL_SCHEMA = NumberSchema.builder()
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "bytes");
                put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Decimal.LOGICAL_NAME);
                put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                put(JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP, Collections.singletonMap("scale", "17"));
            }})
            .build();
    static final org.everit.json.schema.Schema STRING_HIGH_PRECISION_DECIMAL_SCHEMA = StringSchema.builder()
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "bytes");
                put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Decimal.LOGICAL_NAME);
                put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                put(JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP, Collections.singletonMap("scale", "17"));
            }})
            .build();
    static final BigDecimal CONNECT_HIGH_PRECISION_DECIMAL_VALUE =
            new BigDecimal("79228162514264337593543950335.23456789123456789");
    static final JsonNode NUMERIC_HIGH_PRECISION_DECIMAL_JSON_NODE =
            DecimalNode.valueOf(CONNECT_HIGH_PRECISION_DECIMAL_VALUE);
    static final JsonNode BASE64_HIGH_PRECISION_DECIMAL_JSON_NODE = JSON_NODE_FACTORY.binaryNode(
            CONNECT_HIGH_PRECISION_DECIMAL_VALUE.unscaledValue()
                    .toByteArray());

    static final Schema CONNECT_DATE_SCHEMA = Date.builder()
            .build();
    static final org.everit.json.schema.Schema DATE_SCHEMA = NumberSchema.builder()
            .requiresInteger(true)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Date.LOGICAL_NAME);
                put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int32");
            }})
            .build();
    static final java.util.Date CONNECT_DATE_VALUE = Date.toLogical(CONNECT_DATE_SCHEMA, 1620233761);
    static final JsonNode DATE_JSON_NODE = JSON_NODE_FACTORY.numberNode(1620233761);

    static final Schema CONNECT_TIME_SCHEMA = Time.builder()
            .build();
    static final org.everit.json.schema.Schema TIME_SCHEMA = NumberSchema.builder()
            .requiresInteger(true)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Time.LOGICAL_NAME);
                put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int32");
            }})
            .build();
    static final java.util.Date CONNECT_TIME_VALUE = Time.toLogical(CONNECT_TIME_SCHEMA, 86400000);
    static final JsonNode TIME_JSON_NODE = JSON_NODE_FACTORY.numberNode(86400000);

    static final Schema CONNECT_TIMESTAMP_SCHEMA = Timestamp.builder()
            .build();
    static final org.everit.json.schema.Schema TIMESTAMP_SCHEMA = NumberSchema.builder()
            .requiresInteger(true)
            .unprocessedProperties(new HashMap<String, Object>() {{
                put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Timestamp.LOGICAL_NAME);
                put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int64");
            }})
            .build();
    static final java.util.Date CONNECT_TIMESTAMP_VALUE = Timestamp.toLogical(CONNECT_TIMESTAMP_SCHEMA, 1620233761102L);
    static final JsonNode TIMESTAMP_JSON_NODE = JSON_NODE_FACTORY.numberNode(1620233761102L);

    static final Schema CONNECT_UNION_SCHEMA = SchemaBuilder.struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", Schema.INT32_SCHEMA)
            .field("field2", Schema.STRING_SCHEMA)
            .build();
    static final Struct CONNECT_UNION_VALUE_1 = new Struct(CONNECT_UNION_SCHEMA).put("field1", 42);
    static final Struct CONNECT_UNION_VALUE_2 = new Struct(CONNECT_UNION_SCHEMA).put("field2", "answer");
    static final org.everit.json.schema.Schema EXPECTED_JSON_UNION_SCHEMA = CombinedSchema.oneOf(
            Arrays.asList(buildSchemaWithIndex(INT_SCHEMA_BUILDER, 0), buildSchemaWithIndex(StringSchema.builder(), 1)))
            .build();
    static final Schema CONNECT_UNION_SCHEMA_BYTE_STRING = SchemaBuilder.struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", Schema.STRING_SCHEMA)
            .field("field2", Schema.INT8_SCHEMA)
            .build();
    static final Struct CONNECT_UNION_BYTE_STRING_VALUE_1 =
            new Struct(CONNECT_UNION_SCHEMA_BYTE_STRING).put("field1", "answer");
    static final Struct CONNECT_UNION_BYTE_STRING_VALUE_2 =
            new Struct(CONNECT_UNION_SCHEMA_BYTE_STRING).put("field2", (byte) 42);
    static final org.everit.json.schema.Schema EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA = CombinedSchema.oneOf(
            Arrays.asList(buildSchemaWithIndex(StringSchema.builder(), 0),
                          buildSchemaWithIndex(BYTE_SCHEMA_BUILDER, 1)))
            .build();
    static final Schema CONNECT_UNION_INT_BYTE_SCHEMA = SchemaBuilder.struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", Schema.INT32_SCHEMA)
            .field("field2", Schema.INT8_SCHEMA)
            .build();
    static final Struct CONNECT_UNION_INT_BYTE_VALUE_1 = new Struct(CONNECT_UNION_INT_BYTE_SCHEMA).put("field1", 42);
    static final Struct CONNECT_UNION_INT_BYTE_VALUE_2 =
            new Struct(CONNECT_UNION_INT_BYTE_SCHEMA).put("field2", (byte) 10);
    static final org.everit.json.schema.Schema EXPECTED_JSON_INT_BYTE_SCHEMA = CombinedSchema.oneOf(
            Arrays.asList(buildSchemaWithIndex(INT_SCHEMA_BUILDER, 0), buildSchemaWithIndex(BYTE_SCHEMA_BUILDER, 1)))
            .build();

    static final Schema CONNECT_UNION_SCHEMA_MIXED = SchemaBuilder.struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", Schema.FLOAT32_SCHEMA)
            .field("field2", Schema.BOOLEAN_SCHEMA)
            .build();
    static final Struct CONNECT_UNION_MIXED_VALUE_1 = new Struct(CONNECT_UNION_SCHEMA_MIXED).put("field1", 17.17f);
    static final Struct CONNECT_UNION_MIXED_VALUE_2 = new Struct(CONNECT_UNION_SCHEMA_MIXED).put("field2", true);
    static final org.everit.json.schema.Schema EXPECTED_JSON_MIXED_UNION_SCHEMA = CombinedSchema.oneOf(
            Arrays.asList(buildSchemaWithIndex(FLOAT_SCHEMA_BUILDER, 0),
                          buildSchemaWithIndex(BooleanSchema.builder(), 1)))
            .build();
    static final Schema CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES = SchemaBuilder.struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", CONNECT_MAP_SCHEMA_WITH_INTEGER_KEY)
            .field("field2", SchemaBuilder.array(Schema.STRING_SCHEMA)
                    .build())
            .build();
    static final Struct CONNECT_UNION_NON_PRIMITIVE_VALUE_1 =
            new Struct(CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES).put("field1", Collections.singletonMap(42, 42));
    static final Struct CONNECT_UNION_NON_PRIMITIVE_VALUE_2 =
            new Struct(CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES).put("field2", Arrays.asList("a", "b", "c"));
    static final org.everit.json.schema.Schema EXPECTED_JSON_UNION_SCHEMA_NON_PRIMITIVES = CombinedSchema.oneOf(
            Arrays.asList(buildSchemaWithIndex(MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY_BUILDER, 0),
                          buildSchemaWithIndex(ARRAY_SCHEMA_BUILDER, 1)))
            .build();
    static final Schema CONNECT_STRUCT_SCHEMA_FOR_MISSING_FIELDS = SchemaBuilder.struct()
            .field("int32", Schema.INT32_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .build();
    static final Struct CONNECT_STRUCT_WITH_MISSING_FIELDS =
            new Struct(CONNECT_STRUCT_SCHEMA_FOR_MISSING_FIELDS).put("int32", 42)
                    .put("boolean", true);
    static final org.everit.json.schema.Schema JSON_SCHEMA_FOR_MISSING_FIELDS = ObjectSchema.builder()
            .addPropertySchema("int32", buildSchemaWithIndex(INT_SCHEMA_BUILDER, 0))
            .addPropertySchema("boolean", buildSchemaWithIndex(BooleanSchema.builder(), 1))
            .addPropertySchema("string", buildSchemaWithIndex(StringSchema.builder(), 2))
            .build();
    static final ObjectNode JSON_NODE_WITH_MISSING_FIELDS = JSON_NODE_FACTORY.objectNode()
            .put("int32", 42)
            .put("boolean", true);
    static final ArrayNode ARRAY_NODE = JsonNodeFactory.instance.arrayNode()
            .add("a")
            .add("b")
            .add("c");

    static final org.everit.json.schema.Schema BOOLEAN_SCHEMA_WITH_DEFAULT = BooleanSchema.builder()
            .defaultValue(true)
            .build();

    static final Schema CONNECT_BOOLEAN_SCHEMA_WITH_DEFAULT = SchemaBuilder.bool()
            .defaultValue(true)
            .build();

    static org.everit.json.schema.Schema buildSchemaWithIndex(org.everit.json.schema.Schema.Builder schemaBuilder,
                                                              Integer connectIndex) {
        Map<String, Object> unprocessedProperties = schemaBuilder.unprocessedProperties;
        if (connectIndex != null) {
            unprocessedProperties.put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, connectIndex);
        }
        return schemaBuilder.unprocessedProperties(unprocessedProperties)
                .build();

    }

    static Stream<Arguments> testSchemaAndValueArgumentsProvider() {
        return Stream.of(Arguments.of(NULL_SCHEMA, null, NULL_NODE, null),
                         Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, BooleanNode.getTrue(), true),
                         Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, BooleanNode.getFalse(), false),
                         Arguments.of(OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA, NULL_NODE, null),
                         Arguments.of(BOOLEAN_SCHEMA_WITH_DEFAULT, CONNECT_BOOLEAN_SCHEMA_WITH_DEFAULT, NULL_NODE,
                                      null),
                         Arguments.of(OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA, BooleanNode.getTrue(),
                                      true),
                         Arguments.of(OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA, BooleanNode.getFalse(),
                                      false), Arguments.of(OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
                                                           CONNECT_OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT, NULL_NODE,
                                                           null),
                         Arguments.of(OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
                                      CONNECT_OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT, BooleanNode.getTrue(), true),
                         Arguments.of(OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
                                      CONNECT_OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT, BooleanNode.getFalse(), false),
                         Arguments.of(BYTE_SCHEMA, Schema.INT8_SCHEMA, JSON_NODE_FACTORY.numberNode((byte) 42),
                                      (byte) 42),
                         Arguments.of(OPTIONAL_BYTE_SCHEMA, Schema.OPTIONAL_INT8_SCHEMA, NULL_NODE, null),
                         Arguments.of(SHORT_SCHEMA, Schema.INT16_SCHEMA, JSON_NODE_FACTORY.numberNode((short) 42),
                                      (short) 42),
                         Arguments.of(OPTIONAL_SHORT_SCHEMA, Schema.OPTIONAL_INT16_SCHEMA, NULL_NODE, null),
                         Arguments.of(INT_SCHEMA, Schema.INT32_SCHEMA, JSON_NODE_FACTORY.numberNode(42), 42),
                         Arguments.of(OPTIONAL_INT_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA, NULL_NODE, null),
                         Arguments.of(LONG_SCHEMA, Schema.INT64_SCHEMA, JSON_NODE_FACTORY.numberNode(42L), 42L),
                         Arguments.of(OPTIONAL_LONG_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA, NULL_NODE, null),
                         Arguments.of(FLOAT_SCHEMA, Schema.FLOAT32_SCHEMA, JSON_NODE_FACTORY.numberNode(42.42f),
                                      42.42f),
                         Arguments.of(FLOAT_SCHEMA, Schema.FLOAT32_SCHEMA, JSON_NODE_FACTORY.numberNode(1.7f), 1.7f),
                         Arguments.of(OPTIONAL_FLOAT_SCHEMA, Schema.OPTIONAL_FLOAT32_SCHEMA, NULL_NODE, null),
                         Arguments.of(DOUBLE_SCHEMA, Schema.FLOAT64_SCHEMA, JSON_NODE_FACTORY.numberNode(42.42), 42.42),
                         Arguments.of(DOUBLE_SCHEMA, Schema.FLOAT64_SCHEMA, JSON_NODE_FACTORY.numberNode(1.7), 1.7),
                         Arguments.of(OPTIONAL_DOUBLE_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA, NULL_NODE, null),
                         Arguments.of(BYTES_SCHEMA, Schema.BYTES_SCHEMA,
                                      JSON_NODE_FACTORY.binaryNode("answer".getBytes()), "answer".getBytes()),
                         Arguments.of(OPTIONAL_BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA, NULL_NODE, null),
                         Arguments.of(STRING_SCHEMA, Schema.STRING_SCHEMA, JSON_NODE_FACTORY.textNode("answer"),
                                      "answer"),
                         Arguments.of(OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, NULL_NODE, null),
                         Arguments.of(ENUM_SCHEMA, CONNECT_ENUM_SCHEMA, JSON_NODE_FACTORY.textNode("apple"), "apple"),
                         Arguments.of(ENUM_SCHEMA, CONNECT_ENUM_SCHEMA, JSON_NODE_FACTORY.textNode("mango"), "mango"),
                         Arguments.of(ENUM_SCHEMA, CONNECT_ENUM_SCHEMA, JSON_NODE_FACTORY.textNode("banana"), "banana"),
                         Arguments.of(MAP_SCHEMA_WITH_STRING_KEY, CONNECT_MAP_SCHEMA_WITH_STRING_KEY,
                                      JSON_NODE_FACTORY.objectNode()
                                              .put("answer", 42), Collections.singletonMap("answer", 42)),
                         Arguments.of(MAP_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY,
                                      SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
                                              .build(), MAP_JSON_DATA_AS_ARRAY_WITH_STRING_KEY,
                                      Collections.singletonMap("answer", 42)),
                         Arguments.of(MAP_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY,
                                      SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
                                              .build(), MAP_JSON_DATA_AS_ARRAY_WITH_NULL_KEY,
                                      Collections.singletonMap(null, 42)),
                         Arguments.of(MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY,
                                      SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
                                              .build(), MAP_JSON_DATA_AS_ARRAY_WITH_INTEGER_KEY,
                                      Collections.singletonMap(42, 42)),
                         Arguments.of(MAP_NAMED_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY, CONNECT_NAMED_MAP_SCHEMA,
                                      MAP_JSON_DATA_AS_ARRAY_WITH_STRING_KEY, Collections.singletonMap("answer", 42)),
                         Arguments.of(JSON_STRUCT_SCHEMA, CONNECT_STRUCT_SCHEMA, JSON_STRUCT_DATA,
                                      CONNECT_STRUCT_VALUE),
                         Arguments.of(JSON_STRUCT_OPTIONAL_SCHEMA, CONNECT_OPTIONAL_STRUCT_SCHEMA, JSON_STRUCT_DATA,
                                      CONNECT_STRUCT_OPTIONAL_VALUE), Arguments.of(ARRAY_SCHEMA, SchemaBuilder.array(
                        Schema.STRING_SCHEMA)
                        .build(), ARRAY_NODE, Arrays.asList("a", "b", "c")),
                         Arguments.of(NUMBER_DECIMAL_SCHEMA, CONNECT_DECIMAL_SCHEMA, NUMERIC_DECIMAL_JSON_NODE,
                                      CONNECT_DECIMAL_VALUE),
                         Arguments.of(NUMBER_HIGH_PRECISION_DECIMAL_SCHEMA, CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA,
                                      NUMERIC_HIGH_PRECISION_DECIMAL_JSON_NODE, CONNECT_HIGH_PRECISION_DECIMAL_VALUE),
                         Arguments.of(NUMBER_DECIMAL_SCHEMA, CONNECT_DECIMAL_SCHEMA,
                                      NUMERIC_DECIMAL_JSON_NODE_TRAILING_ZEROES, CONNECT_DECIMAL_VALUE_TRAILING_ZEROES),
                         Arguments.of(DATE_SCHEMA, CONNECT_DATE_SCHEMA, DATE_JSON_NODE, CONNECT_DATE_VALUE),
                         Arguments.of(TIME_SCHEMA, CONNECT_TIME_SCHEMA, TIME_JSON_NODE, CONNECT_TIME_VALUE),
                         Arguments.of(TIMESTAMP_SCHEMA, CONNECT_TIMESTAMP_SCHEMA, TIMESTAMP_JSON_NODE,
                                      CONNECT_TIMESTAMP_VALUE),
                         Arguments.of(EXPECTED_JSON_UNION_SCHEMA, CONNECT_UNION_SCHEMA,
                                      JSON_NODE_FACTORY.numberNode(42), CONNECT_UNION_VALUE_1),
                         Arguments.of(EXPECTED_JSON_UNION_SCHEMA, CONNECT_UNION_SCHEMA,
                                      JSON_NODE_FACTORY.textNode("answer"), CONNECT_UNION_VALUE_2),
                         Arguments.of(EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA, CONNECT_UNION_SCHEMA_BYTE_STRING,
                                      JSON_NODE_FACTORY.textNode("answer"), CONNECT_UNION_BYTE_STRING_VALUE_1),
                         Arguments.of(EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA, CONNECT_UNION_SCHEMA_BYTE_STRING,
                                      JSON_NODE_FACTORY.numberNode((byte) 42), CONNECT_UNION_BYTE_STRING_VALUE_2),
                         Arguments.of(EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA, CONNECT_UNION_SCHEMA_BYTE_STRING,
                                      JSON_NODE_FACTORY.textNode("answer"), CONNECT_UNION_BYTE_STRING_VALUE_1),
                         Arguments.of(EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA, CONNECT_UNION_SCHEMA_BYTE_STRING,
                                      JSON_NODE_FACTORY.numberNode((byte) 42), CONNECT_UNION_BYTE_STRING_VALUE_2),
                         Arguments.of(EXPECTED_JSON_MIXED_UNION_SCHEMA, CONNECT_UNION_SCHEMA_MIXED,
                                      JSON_NODE_FACTORY.numberNode(17.17f), CONNECT_UNION_MIXED_VALUE_1),
                         Arguments.of(EXPECTED_JSON_MIXED_UNION_SCHEMA, CONNECT_UNION_SCHEMA_MIXED,
                                      JSON_NODE_FACTORY.booleanNode(true), CONNECT_UNION_MIXED_VALUE_2),
                         Arguments.of(EXPECTED_JSON_UNION_SCHEMA_NON_PRIMITIVES, CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES,
                                      MAP_JSON_DATA_AS_ARRAY_WITH_INTEGER_KEY, CONNECT_UNION_NON_PRIMITIVE_VALUE_1),
                         Arguments.of(EXPECTED_JSON_UNION_SCHEMA_NON_PRIMITIVES, CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES,
                                      ARRAY_NODE, CONNECT_UNION_NON_PRIMITIVE_VALUE_2),
                         Arguments.of(JSON_SCHEMA_FOR_MISSING_FIELDS, CONNECT_STRUCT_SCHEMA_FOR_MISSING_FIELDS,
                                      JSON_NODE_WITH_MISSING_FIELDS, CONNECT_STRUCT_WITH_MISSING_FIELDS));
    }

    static Stream<Arguments> testInvalidSchemaAndValueArgumentsProvider() {
        return Stream.of(Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, null),
                         Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, 42),
                         Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, Arrays.asList(42)),
                         Arguments.of(BYTE_SCHEMA, Schema.INT8_SCHEMA, 42),
                         Arguments.of(INT_SCHEMA, Schema.INT32_SCHEMA, 42L),
                         Arguments.of(INT_SCHEMA, Schema.INT32_SCHEMA, 42.42f),
                         Arguments.of(FLOAT_SCHEMA, Schema.FLOAT32_SCHEMA, 42.42),
                         Arguments.of(OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, 123),
                         Arguments.of(MAP_SCHEMA_WITH_STRING_KEY, CONNECT_MAP_SCHEMA_WITH_STRING_KEY,
                                      Collections.singletonMap(true, 42)),
                         Arguments.of(MAP_SCHEMA_WITH_STRING_KEY, CONNECT_MAP_SCHEMA_WITH_STRING_KEY,
                                      Collections.singletonMap("answer", 42L)),
                         Arguments.of(MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY, CONNECT_MAP_SCHEMA_WITH_INTEGER_KEY,
                                      Collections.singletonMap("answer", 42)), Arguments.of(ARRAY_SCHEMA,
                                                                                            SchemaBuilder.array(
                                                                                                    Schema.STRING_SCHEMA)
                                                                                                    .build(),
                                                                                            Arrays.asList(1, 2, 3)),
                         Arguments.of(BYTES_SCHEMA, Schema.BYTES_SCHEMA, "answer"),
                         Arguments.of(JSON_STRUCT_SCHEMA, CONNECT_STRUCT_SCHEMA,
                                      new Struct(CONNECT_STRUCT_STRING_SCHEMA).put("int32", "42")),
                         Arguments.of(DATE_SCHEMA, CONNECT_DATE_SCHEMA, CONNECT_TIMESTAMP_VALUE),
                         Arguments.of(DATE_SCHEMA, CONNECT_DATE_SCHEMA, 86400000),
                         Arguments.of(NUMBER_DECIMAL_SCHEMA, CONNECT_DECIMAL_SCHEMA, 156.00),
                         Arguments.of(EXPECTED_JSON_INT_BYTE_SCHEMA, CONNECT_UNION_INT_BYTE_SCHEMA,
                                      JSON_NODE_FACTORY.numberNode(42), CONNECT_UNION_INT_BYTE_VALUE_1),
                         Arguments.of(EXPECTED_JSON_INT_BYTE_SCHEMA, CONNECT_UNION_INT_BYTE_SCHEMA,
                                      JSON_NODE_FACTORY.numberNode((byte) 10), CONNECT_UNION_INT_BYTE_VALUE_2),
                         Arguments.of(EXPECTED_JSON_UNION_SCHEMA_NON_PRIMITIVES, CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES,
                                      Collections.singletonMap(true, 42)));
    }
}
