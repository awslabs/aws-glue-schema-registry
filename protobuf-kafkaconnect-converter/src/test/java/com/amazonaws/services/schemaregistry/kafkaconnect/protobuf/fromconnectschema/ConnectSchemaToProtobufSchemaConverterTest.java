package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ConnectSchemaToProtobufSchemaConverter;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getPrimitiveSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getEnumSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getProtobufSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConnectSchemaToProtobufSchemaConverterTest {

    private static final ConnectSchemaToProtobufSchemaConverter CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER =
        new ConnectSchemaToProtobufSchemaConverter();

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
                getPrimitiveSchema("PrimitiveTypes"),
                getProtobufSchema("PrimitiveProtobufSchema.filedescproto")
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
    public void fromConnectSchema_convertsConnectSchemaToProtobufSchema(String fileName, Schema connectSchema,
        String expectedProtobufSchema) {

        final Descriptors.FileDescriptor protobufSchema =
            CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema);

        final String actualSchema = protobufSchema.toProto().toString();

        assertEquals(expectedProtobufSchema, actualSchema);
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

        assertThrows(DataException.class,
            () -> CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema));
    }

    private static Map<String, Schema> getEnumTypesForExceptions() {
        return ImmutableMap.<String, Schema>builder()
                .put("nonNumberTag", new SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "jsf").build())
                .put("nullNumberTag", new SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, null).build())
                .put("invalidInt32Metadata", new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "int64").build())
                .put("invalidInt64Metadata",
                        new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "string").build())
                .put("corpus", new SchemaBuilder(Schema.Type.STRING).parameter("protobuf.type", "enum")
                        .parameter("PROTOBUF_SCHEMA_ENUM.com.amazonaws.services.schemaregistry.kafkaconnect.tests.enumsyntax3.EnumTest.Corpus.UNIVERSAL", "0")
                        .parameter("PROTOBUF_SCHEMA_ENUM.com.amazonaws.services.schemaregistry.kafkaconnect.tests.enumsyntax3.EnumTest.Corpus.WEB", "1")
                        .parameter("PROTOBUF_SCHEMA_ENUM.com.amazonaws.services.schemaregistry.kafkaconnect.tests.enumsyntax3.EnumTest.Corpus.IMAGES", "2")
                        .parameter("PROTOBUF_SCHEMA_ENUM.com.amazonaws.services.schemaregistry.kafkaconnect.tests.enumsyntax3.EnumTest.Corpus.LOCAL", "3")
                        .parameter("PROTOBUF_SCHEMA_ENUM.com.amazonaws.services.schemaregistry.kafkaconnect.tests.enumsyntax3.EnumTest.Corpus.NEWS", "4")
                        .parameter("PROTOBUF_SCHEMA_ENUM.com.amazonaws.services.schemaregistry.kafkaconnect.tests.enumsyntax3.EnumTest.Corpus.PRODUCTS", "5")
                        .parameter("PROTOBUF_SCHEMA_ENUM.com.amazonaws.services.schemaregistry.kafkaconnect.tests.enumsyntax3.EnumTest.Corpus.VIDEO", "6")
                        .parameter("ENUM_NAME", "corpus")
                        .parameter("protobuf.tag", "4")
                        .optional()
                        .build())
                .build();
    }

    private static Stream<Arguments> getEnumSchemaTestCases() {
        return Stream.of(
                Arguments.of(
                        "EnumTypes",
                        getEnumSchema("EnumTypes"),
                        getProtobufSchema("EnumProtobufSchema.filedescproto")
                )
        );
    }

    private static Stream<Arguments> getEnumSchemaExceptionTestCases() {
        return getEnumTypesForExceptions()
                .entrySet()
                .stream()
                .map(entry ->
                        Arguments.of(entry.getKey(), ImmutableMap.of(entry.getKey(), entry.getValue()))
                );
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("getEnumSchemaTestCases")
    public void fromConnectSchema_convertsConnectSchemaToProtobufSchemaEnum(String fileName, Schema connectSchema,
                                                                        String expectedProtobufSchema) {

        final Descriptors.FileDescriptor protobufSchema =
                CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema);

        final String actualSchema = protobufSchema.toProto().toString();

        assertEquals(expectedProtobufSchema, actualSchema);
    }



}