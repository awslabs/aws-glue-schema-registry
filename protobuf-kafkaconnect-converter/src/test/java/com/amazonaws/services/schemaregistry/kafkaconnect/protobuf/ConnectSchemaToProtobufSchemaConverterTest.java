package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.schematypeconverter.ProtobufSchemaConverterConstants.PROTOBUF_TAG;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.schematypeconverter.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConnectSchemaToProtobufSchemaConverterTest {

    private static final ConnectSchemaToProtobufSchemaConverter CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER =
        new ConnectSchemaToProtobufSchemaConverter();
    private static final String TEST_RESOURCE_PATH = "src/test/resources/";

    private static Map<String, Schema> getPrimitiveTypes() {
        return ImmutableMap.<String, Schema>builder()
            .put("i8", new SchemaBuilder(Schema.Type.INT8).build())
            .put("i8WithParam", new SchemaBuilder(Schema.Type.INT8).parameter(PROTOBUF_TAG, "2000").build())
            .put("i8Optional", new SchemaBuilder(Schema.Type.INT8).optional().build())
            .put("i8WithDefault", new SchemaBuilder(Schema.Type.INT8).defaultValue((byte) 1).build())
            .put("i16", new SchemaBuilder(Schema.Type.INT16).build())
            .put("i16WithParam", new SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "4123").build())
            .put("i16WithDefault", new SchemaBuilder(Schema.Type.INT16).defaultValue((short) 890).build())
            .put("i16Optional", new SchemaBuilder(Schema.Type.INT16).optional().build())
            .put("i32", new SchemaBuilder(Schema.Type.INT32).build())
            .put("i32WithParam", new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "8123").build())
            .put("i32WithSameTypeMetadata",
                new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "INT32").build())
            .put("i32WithMetadata", new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "sint32").build())
            .put("i32WithAnotherMetadata",
                new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "sfixed32").build())
            .put("i32WithDefault", new SchemaBuilder(Schema.Type.INT32).defaultValue(21233).build())
            .put("i32Optional", new SchemaBuilder(Schema.Type.INT32).optional().build())
            .put("i64", new SchemaBuilder(Schema.Type.INT64).build())
            .put("i64WithParam", new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TAG, "9123").build())
            .put("i64WithDefault", new SchemaBuilder(Schema.Type.INT64).defaultValue(Long.MAX_VALUE).build())
            .put("i64WithSameTypeMetadata",
                new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TYPE, "int64").build())
            .put("i64WithMetadata", new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TYPE, "sint64").build())
            .put("i64WithAnotherMetadata",
                new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TYPE, "fixed64").build())
            .put("i64WithYetAnotherMetadata",
                new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TYPE, "uint32").build())
            .put("i64Optional", new SchemaBuilder(Schema.Type.INT64).optional().build())
            .put("f32", new SchemaBuilder(Schema.Type.FLOAT32).build())
            .put("f32WithParam", new SchemaBuilder(Schema.Type.FLOAT32).parameter(PROTOBUF_TAG, "10923").build())
            .put("f32Optional", new SchemaBuilder(Schema.Type.FLOAT32).optional().build())
            .put("f32WithDefault", new SchemaBuilder(Schema.Type.FLOAT32).defaultValue(123.2324f).build())
            .put("f64", new SchemaBuilder(Schema.Type.FLOAT64).build())
            .put("f64WithParam", new SchemaBuilder(Schema.Type.FLOAT64).parameter(PROTOBUF_TAG, "1112").build())
            .put("f64Optional", new SchemaBuilder(Schema.Type.FLOAT64).optional().build())
            .put("f64WithDefault", new SchemaBuilder(Schema.Type.FLOAT64).defaultValue(123112322232.2644).build())
            .put("bool", new SchemaBuilder(Schema.Type.BOOLEAN).build())
            .put("boolWithParam", new SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "12123").build())
            .put("boolOptional", new SchemaBuilder(Schema.Type.BOOLEAN).optional().build())
            .put("boolWithDefault", new SchemaBuilder(Schema.Type.BOOLEAN).defaultValue(true).build())
            .put("bytes", new SchemaBuilder(Schema.Type.BYTES).build())
            .put("bytesWithParam", new SchemaBuilder(Schema.Type.BYTES).parameter(PROTOBUF_TAG, "18924").build())
            .put("bytesOptional", new SchemaBuilder(Schema.Type.BYTES).optional().build())
            .put("bytesWithDefault",
                new SchemaBuilder(Schema.Type.BYTES).defaultValue(new byte[] { 3, 4, 'f', 123 }).build())
            .put("str", new SchemaBuilder(Schema.Type.STRING).build())
            .put("strWithParam", new SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "13912").build())
            .put("strOptional", new SchemaBuilder(Schema.Type.STRING).optional().build())
            .put("strWithDefault", new SchemaBuilder(Schema.Type.STRING).defaultValue("foobarxyz").build())
            .build();
    }

    private static Map<String, Schema> getPrimitiveTypesForExceptions() {
        return ImmutableMap.<String, Schema>builder()
            .put("nonNumberTag", new SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "jsf").build())
            .put("nullNumberTag", new SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, null).build())
            .put("invalidInt32Metadata", new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "int64").build())
            .put("invalidInt64Metadata",
                new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "string").build())
            .build();
    }

    private static Stream<Arguments> getConnectSchemaTestCases() {
        return Stream.of(
            Arguments.of(
                "PrimitiveTypes",
                getPrimitiveTypes(),
                "PrimitiveProtobufSchema.filedescproto"
            )
        );
    }

    private static Stream<Arguments> getConnectSchemaExceptionTestCases() {
        return getPrimitiveTypesForExceptions()
            .entrySet()
            .stream()
            .map(entry ->
                Arguments.of(entry.getKey(), ImmutableMap.of(entry.getKey(), entry.getValue()))
            );
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("getConnectSchemaTestCases")
    public void fromConnectSchema_convertsConnectSchemaToProtobufSchema(String fileName,
        Map<String, Schema> connectTypes, String expectedProtobufSchemaFile)
        throws IOException {
        final SchemaBuilder parentSchemaBuilder = new SchemaBuilder(Schema.Type.STRUCT);
        parentSchemaBuilder.name(fileName);

        connectTypes
            .forEach(parentSchemaBuilder::field);

        final Schema connectSchema = parentSchemaBuilder.build();

        final Descriptors.FileDescriptor protobufSchema =
            CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema);

        final String actualSchema = protobufSchema.toProto().toString();

        final String expectedSchema =
            new String(Files.readAllBytes(Paths.get(TEST_RESOURCE_PATH, expectedProtobufSchemaFile)),
                Charset.defaultCharset());

        assertEquals(expectedSchema, actualSchema);
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("getConnectSchemaExceptionTestCases")
    public void fromConnectSchema_convertsConnectSchemaToProtobufSchema_forExceptions(String fileName,
        Map<String, Schema> connectTypes) {
        final SchemaBuilder parentSchemaBuilder = new SchemaBuilder(Schema.Type.STRUCT);
        parentSchemaBuilder.name(fileName);

        connectTypes
            .forEach(parentSchemaBuilder::field);

        final Schema connectSchema = parentSchemaBuilder.build();

        assertThrows(IllegalArgumentException.class,
            () -> CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema));
    }

    @Test
    public void fromConnectSchema_onNullSchema_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(null));
    }
}