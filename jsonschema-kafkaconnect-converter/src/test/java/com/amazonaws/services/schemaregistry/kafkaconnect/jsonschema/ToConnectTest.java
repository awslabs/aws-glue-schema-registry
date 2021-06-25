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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.DecimalFormat;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ToConnectTest {
    private static final JsonNodeFactory JSON_NODE_FACTORY = TypeConverter.JSON_NODE_FACTORY;
    private JsonNodeToConnectValueConverter jsonNodeToConnectValueConverter;
    private JsonSchemaToConnectSchemaConverter jsonSchemaToConnectSchemaConverter;

    @BeforeEach
    public void setUp() {
        JsonSchemaDataConfig jsonSchemaDataConfig = new JsonSchemaDataConfig(
                Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()));
        jsonNodeToConnectValueConverter = new JsonNodeToConnectValueConverter(jsonSchemaDataConfig);
        jsonSchemaToConnectSchemaConverter = new JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig);
    }

    @ParameterizedTest
    @MethodSource(value = "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#"
                          + "testSchemaAndValueArgumentsProvider")
    public void testToConnect_schemaAndValue_asExpected(org.everit.json.schema.Schema jsonSchema,
                                                        Schema connectSchema,
                                                        JsonNode jsonValue,
                                                        Object expectedConnectValue) {
        Object actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue);

        if (expectedConnectValue != null && expectedConnectValue.getClass()
                .isArray() && Schema.Type.BYTES.equals(connectSchema.type())) {
            assertArrayEquals((byte[]) expectedConnectValue, (byte[]) actualConnectValue);
        } else if (!jsonValue.isNull() || !jsonSchema.hasDefaultValue()) {
            assertEquals(expectedConnectValue, actualConnectValue);
        }

        Schema actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema);

        assertEquals(connectSchema, actualConnectSchema);
    }

    @Test
    public void testToConnect_base64Decimal_asExpected() {
        JsonSchemaDataConfig jsonSchemaDataConfig = new JsonSchemaDataConfig(
                Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.BASE64.name()));
        JsonNodeToConnectValueConverter jsonNodeToConnectValueConverter =
                new JsonNodeToConnectValueConverter(jsonSchemaDataConfig);

        Object actualConnectValue =
                jsonNodeToConnectValueConverter.toConnectValue(TestDataProvider.CONNECT_DECIMAL_SCHEMA,
                                                               TestDataProvider.BASE64_DECIMAL_JSON_NODE);

        assertEquals(TestDataProvider.CONNECT_DECIMAL_VALUE, actualConnectValue);

        Schema actualConnectSchema =
                jsonSchemaToConnectSchemaConverter.toConnectSchema(TestDataProvider.STRING_DECIMAL_SCHEMA);

        assertEquals(TestDataProvider.CONNECT_DECIMAL_SCHEMA, actualConnectSchema);

        ConnectSchema.validateValue(TestDataProvider.CONNECT_DECIMAL_SCHEMA, actualConnectValue);

        Object highPrecisionActualConnectValue =
                jsonNodeToConnectValueConverter.toConnectValue(TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA,
                                                               TestDataProvider.BASE64_HIGH_PRECISION_DECIMAL_JSON_NODE);

        assertEquals(TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_VALUE, highPrecisionActualConnectValue);

        actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(
                TestDataProvider.STRING_HIGH_PRECISION_DECIMAL_SCHEMA);

        assertEquals(TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA, actualConnectSchema);

        ConnectSchema.validateValue(TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA, actualConnectValue);
    }

    @Test
    public void testToConnect_complexStruct_asExpected() {
        Schema connectSchema = SchemaBuilder.struct()
                .field("int8", SchemaBuilder.int8()
                        .defaultValue((byte) 2)
                        .doc("int8 field")
                        .build())
                .field("int16", Schema.INT16_SCHEMA)
                .field("int32", Schema.INT32_SCHEMA)
                .field("int64", Schema.INT64_SCHEMA)
                .field("float32", Schema.FLOAT32_SCHEMA)
                .field("float64", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA)
                        .build())
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
                        .build())
                .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
                        .build())
                .build();
        Struct connectValue = new Struct(connectSchema).put("int8", (byte) 42)
                .put("int16", (short) 42)
                .put("int32", 42)
                .put("int64", 42L)
                .put("float32", 42.42f)
                .put("float64", 42.42)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", "foo".getBytes())
                .put("array", Arrays.asList("a", "b", "c"))
                .put("map", Collections.singletonMap("field", 1))
                .put("mapNonStringKeys", Collections.singletonMap(1, 1));

        org.everit.json.schema.Schema complexMapElementSchema = ArraySchema.builder()
                .allItemSchema(ObjectSchema.builder()
                                       .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD,
                                                          TestDataProvider.INT_SCHEMA)
                                       .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD,
                                                          TestDataProvider.INT_SCHEMA)
                                       .build())
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "map");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 11);
                }})
                .build();

        // One field has some extra data set on it to ensure it gets passed through via the fields
        // config
        org.everit.json.schema.Schema byteSchemaWithDefault = NumberSchema.builder()
                .requiresInteger(true)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int8");
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "int8 field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 0);
                }})
                .defaultValue((byte) 2)
                .build();

        org.everit.json.schema.Schema expectedJsonSchema = ObjectSchema.builder()
                .addPropertySchema("int8", byteSchemaWithDefault)
                .addPropertySchema("int16",
                                   TestDataProvider.buildSchemaWithIndex(TestDataProvider.SHORT_SCHEMA_BUILDER, 1))
                .addPropertySchema("int32",
                                   TestDataProvider.buildSchemaWithIndex(TestDataProvider.INT_SCHEMA_BUILDER, 2))
                .addPropertySchema("int64",
                                   TestDataProvider.buildSchemaWithIndex(TestDataProvider.LONG_SCHEMA_BUILDER, 3))
                .addPropertySchema("float32",
                                   TestDataProvider.buildSchemaWithIndex(TestDataProvider.FLOAT_SCHEMA_BUILDER, 4))
                .addPropertySchema("float64",
                                   TestDataProvider.buildSchemaWithIndex(TestDataProvider.DOUBLE_SCHEMA_BUILDER, 5))
                .addPropertySchema("boolean", TestDataProvider.buildSchemaWithIndex(BooleanSchema.builder(), 6))
                .addPropertySchema("string", TestDataProvider.buildSchemaWithIndex(StringSchema.builder(), 7))
                .addPropertySchema("bytes",
                                   TestDataProvider.buildSchemaWithIndex(TestDataProvider.BYTES_SCHEMA_BUILDER, 8))
                .addPropertySchema("array",
                                   TestDataProvider.buildSchemaWithIndex(TestDataProvider.ARRAY_SCHEMA_BUILDER, 9))
                .addPropertySchema("map", TestDataProvider.buildSchemaWithIndex(
                        TestDataProvider.MAP_SCHEMA_WITH_STRING_KEY_BUILDER, 10))
                .addPropertySchema("mapNonStringKeys", complexMapElementSchema)
                .build();

        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        array.add("a")
                .add("b")
                .add("c");
        ObjectNode expectedJsonNode = JSON_NODE_FACTORY.objectNode()
                .put("int8", 42)
                .put("int16", 42)
                .put("int32", 42)
                .put("int64", 42L)
                .put("float32", 42.42f)
                .put("float64", 42.42)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", "foo".getBytes())
                .set("array", array);

        expectedJsonNode.set("map", JSON_NODE_FACTORY.objectNode()
                .put("field", 1));

        expectedJsonNode.set("mapNonStringKeys", JSON_NODE_FACTORY.arrayNode()
                .add(JSON_NODE_FACTORY.objectNode()
                             .put(JsonSchemaConverterConstants.VALUE_FIELD, 1)
                             .put(JsonSchemaConverterConstants.KEY_FIELD, 1)));

        Object actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, expectedJsonNode);

        assertEquals(connectValue, actualConnectValue);

        Schema actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(expectedJsonSchema);

        assertEquals(connectSchema, actualConnectSchema);

        ConnectSchema.validateValue(actualConnectSchema, actualConnectValue);
    }

    @Test
    public void testFromConnectComplex_withDefaults_succeeds() {
        int dateDefVal = 100;
        int timeDefVal = 1000 * 60 * 60 * 2;
        long tsDefVal = 1000 * 60 * 60 * 24 * 365 + 100;
        java.util.Date dateDef = Date.toLogical(Date.SCHEMA, dateDefVal);
        java.util.Date timeDef = Time.toLogical(Time.SCHEMA, timeDefVal);
        java.util.Date tsDef = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal);
        BigDecimal decimalDef = new BigDecimal(BigInteger.valueOf(314159L), 5);

        Schema connectSchema = SchemaBuilder.struct()
                .field("int8", SchemaBuilder.int8()
                        .defaultValue((byte) 42)
                        .doc("int8 field")
                        .build())
                .field("int16", SchemaBuilder.int16()
                        .defaultValue((short) 42)
                        .doc("int16 field")
                        .build())
                .field("int32", SchemaBuilder.int32()
                        .defaultValue(42)
                        .doc("int32 field")
                        .build())
                .field("int64", SchemaBuilder.int64()
                        .defaultValue(42L)
                        .doc("int64 field")
                        .build())
                .field("float32", SchemaBuilder.float32()
                        .defaultValue(42.42f)
                        .doc("float32 field")
                        .build())
                .field("float64", SchemaBuilder.float64()
                        .defaultValue(42.42)
                        .doc("float64 field")
                        .build())
                .field("boolean", SchemaBuilder.bool()
                        .defaultValue(true)
                        .doc("bool field")
                        .build())
                .field("string", SchemaBuilder.string()
                        .defaultValue("foo")
                        .doc("string field")
                        .build())
                .field("bytes", SchemaBuilder.bytes()
                        .defaultValue("foo".getBytes())
                        .doc("bytes field")
                        .build())
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA)
                        .defaultValue(Arrays.asList("a", "b", "c"))
                        .build())
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
                        .defaultValue(Collections.singletonMap("field", 1))
                        .build())
                .field("date", Date.builder()
                        .defaultValue(dateDef)
                        .doc("date field")
                        .build())
                .field("time", Time.builder()
                        .defaultValue(timeDef)
                        .doc("time field")
                        .build())
                .field("ts", Timestamp.builder()
                        .defaultValue(tsDef)
                        .doc("ts field")
                        .build())
                .field("decimal", Decimal.builder(5)
                        .defaultValue(decimalDef)
                        .doc("decimal field")
                        .build())
                .build();
        // leave the struct empty so that only defaults are used
        Struct connectValue = new Struct(connectSchema).put("int8", (byte) 42)
                .put("int16", (short) 42)
                .put("int32", 42)
                .put("int64", 42L)
                .put("float32", 42.42f)
                .put("float64", 42.42)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", "foo".getBytes())
                .put("array", Arrays.asList("a", "b", "c"))
                .put("map", Collections.singletonMap("field", 1))
                .put("date", dateDef)
                .put("time", timeDef)
                .put("ts", tsDef)
                .put("decimal", decimalDef);

        org.everit.json.schema.Schema byteSchemaWithDefault = NumberSchema.builder()
                .requiresInteger(true)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int8");
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "int8 field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 0);
                }})
                .defaultValue((byte) 42)
                .build();

        org.everit.json.schema.Schema shortSchemaWithDefault = NumberSchema.builder()
                .requiresInteger(true)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int16");
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "int16 field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 1);
                }})
                .defaultValue((short) 42)
                .build();

        org.everit.json.schema.Schema intSchemaWithDefault = NumberSchema.builder()
                .requiresInteger(true)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int32");
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "int32 field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 2);
                }})
                .defaultValue(42)
                .build();

        org.everit.json.schema.Schema longSchemaWithDefault = NumberSchema.builder()
                .requiresInteger(true)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int64");
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "int64 field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 3);
                }})
                .defaultValue(42L)
                .build();

        org.everit.json.schema.Schema floatSchemaWithDefault = NumberSchema.builder()
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "float32");
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "float32 field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 4);
                }})
                .defaultValue(42.42f)
                .build();

        org.everit.json.schema.Schema doubleSchemaWithDefault = NumberSchema.builder()
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "float64");
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "float64 field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 5);
                }})
                .defaultValue(42.42)
                .build();

        org.everit.json.schema.Schema boolSchemaWithDefault = BooleanSchema.builder()
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "bool field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 6);
                }})
                .defaultValue(true)
                .build();

        org.everit.json.schema.Schema stringSchemaWithDefault = StringSchema.builder()
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "string field");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 7);
                }})
                .defaultValue("foo")
                .build();

        org.everit.json.schema.Schema bytesSchemaWithDefault = StringSchema.builder()
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "bytes field");
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "bytes");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 8);
                }})
                .defaultValue("foo".getBytes())
                .build();

        org.everit.json.schema.Schema arraySchemaWithDefault = ArraySchema.builder()
                .allItemSchema(TestDataProvider.STRING_SCHEMA)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 9);
                }})
                .defaultValue(Arrays.asList("a", "b", "c"))
                .build();

        org.everit.json.schema.Schema mapSchemaWithDefaultValue = ObjectSchema.builder()
                .schemaOfAdditionalProperties(TestDataProvider.INT_SCHEMA)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "map");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 10);
                }})
                .defaultValue(Collections.singletonMap("field", 1))
                .build();

        org.everit.json.schema.Schema dateSchemaWithDefault = NumberSchema.builder()
                .requiresInteger(true)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Date.LOGICAL_NAME);
                    put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "date field");
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int32");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 11);
                }})
                .defaultValue(dateDef)
                .build();

        org.everit.json.schema.Schema timeSchemaWithDefault = NumberSchema.builder()
                .requiresInteger(true)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Time.LOGICAL_NAME);
                    put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "time field");
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int32");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 12);
                }})
                .defaultValue(timeDef)
                .build();

        org.everit.json.schema.Schema tsSchemaWithDefault = NumberSchema.builder()
                .requiresInteger(true)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Timestamp.LOGICAL_NAME);
                    put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "ts field");
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "int64");
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 13);
                }})
                .defaultValue(tsDef)
                .build();

        org.everit.json.schema.Schema decimalSchemaWithDefault = NumberSchema.builder()
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, "bytes");
                    put(JsonSchemaConverterConstants.CONNECT_NAME_PROP, Decimal.LOGICAL_NAME);
                    put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 1);
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "decimal field");
                    put(JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP, Collections.singletonMap("scale", "5"));
                    put(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, 14);
                }})
                .defaultValue(decimalDef)
                .build();

        org.everit.json.schema.Schema expectedJsonSchema = ObjectSchema.builder()
                .addPropertySchema("int8", byteSchemaWithDefault)
                .addPropertySchema("int16", shortSchemaWithDefault)
                .addPropertySchema("int32", intSchemaWithDefault)
                .addPropertySchema("int64", longSchemaWithDefault)
                .addPropertySchema("float32", floatSchemaWithDefault)
                .addPropertySchema("float64", doubleSchemaWithDefault)
                .addPropertySchema("boolean", boolSchemaWithDefault)
                .addPropertySchema("string", stringSchemaWithDefault)
                .addPropertySchema("bytes", bytesSchemaWithDefault)
                .addPropertySchema("array", arraySchemaWithDefault)
                .addPropertySchema("map", mapSchemaWithDefaultValue)
                .addPropertySchema("date", dateSchemaWithDefault)
                .addPropertySchema("time", timeSchemaWithDefault)
                .addPropertySchema("ts", tsSchemaWithDefault)
                .addPropertySchema("decimal", decimalSchemaWithDefault)
                .build();

        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        array.add("a")
                .add("b")
                .add("c");
        ObjectNode expectedJsonNode = JSON_NODE_FACTORY.objectNode()
                .put("int8", 42)
                .put("int16", 42)
                .put("int32", 42)
                .put("int64", 42L)
                .put("float32", 42.42f)
                .put("float64", 42.42)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", "foo".getBytes())
                .set("array", array);

        expectedJsonNode.set("map", JSON_NODE_FACTORY.objectNode()
                .put("field", 1));

        expectedJsonNode.put("date", dateDefVal)
                .put("time", timeDefVal)
                .put("ts", tsDefVal)
                .put("decimal", decimalDef);

        Object actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, expectedJsonNode);

        assertEquals(connectValue, actualConnectValue);

        Schema actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(expectedJsonSchema);

        assertEquals(connectSchema, actualConnectSchema);

        ConnectSchema.validateValue(connectSchema, actualConnectValue);
    }

    @Test
    public void testToConnectStruct_withMetadata_succeeds() {
        Schema connectSchema = SchemaBuilder.struct()
                .name("com.amazonaws.services.schemaregistry.test.TestSchema")
                .version(12)
                .doc("doc")
                .field("int32", Schema.INT32_SCHEMA)
                .build();
        Struct connectValue = new Struct(connectSchema).put("int32", 42);

        org.everit.json.schema.Schema expectedJsonSchema = ObjectSchema.builder()
                .addPropertySchema("int32", TestDataProvider.INT_SCHEMA)
                .unprocessedProperties(new HashMap<String, Object>() {{
                    put(JsonSchemaConverterConstants.CONNECT_NAME_PROP,
                        "com.amazonaws.services.schemaregistry.test.TestSchema");
                    put(JsonSchemaConverterConstants.CONNECT_VERSION_PROP, 12);
                    put(JsonSchemaConverterConstants.CONNECT_DOC_PROP, "doc");
                }})
                .build();

        JsonNode expectedJsonNode = JSON_NODE_FACTORY.objectNode()
                .put("int32", 42);

        Object actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, expectedJsonNode);

        assertEquals(connectValue, actualConnectValue);

        Schema actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(expectedJsonSchema);

        assertEquals(connectSchema, actualConnectSchema);

        ConnectSchema.validateValue(connectSchema, actualConnectValue);
    }

    @Test
    public void testSchemaCache_size_toConnectConversion() {
        JsonSchemaDataConfig jsonSchemaDataConfig =
                new JsonSchemaDataConfig(Collections.singletonMap(JsonSchemaDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 4));
        JsonSchemaToConnectSchemaConverter jsonSchemaToConnectSchemaConverter =
                new JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig);

        Cache<org.everit.json.schema.Schema, Schema> cache =
                jsonSchemaToConnectSchemaConverter.getToConnectSchemaCache();
        assertEquals(0, cache.size());

        jsonSchemaToConnectSchemaConverter.toConnectSchema(BooleanSchema.builder()
                                                                   .build());
        assertEquals(1, cache.size());

        jsonSchemaToConnectSchemaConverter.toConnectSchema(BooleanSchema.builder()
                                                                   .build());
        assertEquals(1, cache.size());

        jsonSchemaToConnectSchemaConverter.toConnectSchema(TestDataProvider.INT_SCHEMA);
        assertEquals(2, cache.size());

        jsonSchemaToConnectSchemaConverter.toConnectSchema(TestDataProvider.LONG_SCHEMA);
        assertEquals(3, cache.size());

        jsonSchemaToConnectSchemaConverter.toConnectSchema(TestDataProvider.FLOAT_SCHEMA);
        assertEquals(4, cache.size());

        // Should hit limit of cache
        jsonSchemaToConnectSchemaConverter.toConnectSchema(StringSchema.builder()
                                                                   .build());
        assertEquals(4, cache.size());
    }
}
