package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema;

import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArrayProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArraySchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufSchemaToConnectSchemaConverterTest {

    private final static ProtobufSchemaToConnectSchemaConverter PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER =
        new ProtobufSchemaToConnectSchemaConverter();

    private static Stream<Arguments> getPrimitiveTestCases() {
        return getPrimitiveProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getArrayTestCases() {
        return getArrayProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getMapTestCases() {
        return getMapProtobufMessages().stream().map(Arguments::of);
    }

    @BeforeEach
    public void setUp() {
    }

    @ParameterizedTest
    @MethodSource("getPrimitiveTestCases")
    public void toConnectSchema_convertsPrimitiveTypesSchema(Message message) {
        String packageName = message.getDescriptorForType().getFile().getPackage();
        Schema actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message);
        Schema expectedConnectSchema = getPrimitiveSchema(packageName);
        assertEquals(expectedConnectSchema, actualConnectSchema);
    }

    @ParameterizedTest
    @MethodSource("getArrayTestCases")
    public void toConnectSchema_convertsArrayTypeSchema(Message message) {
        String packageName = message.getDescriptorForType().getFile().getPackage();
        Schema actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message);
        Schema expectedConnectSchema = getArraySchema(packageName);
        assertEquals(expectedConnectSchema, actualConnectSchema);
    }

    @ParameterizedTest
    @MethodSource("getMapTestCases")
    public void toConnectSchema_convertsMapTypeSchema(Message message) {
        String packageName = message.getDescriptorForType().getFile().getPackage();
        Schema actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message);
        Schema expectedConnectSchema = getMapSchema(packageName);
        assertEquals(expectedConnectSchema, actualConnectSchema);
    }

    @Test
    public void toConnectSchema_forNullMessage_ThrowsException() {
        assertThrows(IllegalArgumentException.class,
            () -> PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(null));
    }
}