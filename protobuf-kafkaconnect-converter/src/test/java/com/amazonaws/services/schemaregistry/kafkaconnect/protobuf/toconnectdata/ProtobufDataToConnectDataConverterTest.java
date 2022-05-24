package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata;

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArrayProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArraySchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArrayTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveTypesData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getTimeProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getTimeSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getTimeTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getDecimalProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getDecimalSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getDecimalTypeData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufDataToConnectDataConverterTest {
    private static final ProtobufDataToConnectDataConverter PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER =
        new ProtobufDataToConnectDataConverter();

    private static Stream<Arguments> getPrimitiveTestCases() {
        return getPrimitiveProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getEnumTestCases() {
        return getEnumProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getArrayTestCases() {
        return getArrayProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getMapTestCases() {
        return getMapProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getTimeTestCases() {
        return getTimeProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getDecimalTestCases() {
        return getDecimalProtobufMessages().stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("getPrimitiveTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forPrimitiveTypes(Message primitiveMessage) {
        String packageName = primitiveMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getPrimitiveSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(primitiveMessage, connectSchema);
        Struct expectedData = getPrimitiveTypesData(packageName);

        assertEquals(expectedData, actualData);
    }

    @ParameterizedTest
    @MethodSource("getEnumTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forEnumType(Message enumMessage) {
        String packageName = enumMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getEnumSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(enumMessage, connectSchema);
        Struct expectedData = getEnumTypeData(packageName);

        assertEquals(expectedData, actualData);
    }

    @ParameterizedTest
    @MethodSource("getArrayTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forArrayType(Message arrayMessage) {
        String packageName = arrayMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getArraySchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(arrayMessage, connectSchema);
        Struct expectedData = getArrayTypeData(packageName);

        assertEquals(expectedData, actualData);
    }

    @ParameterizedTest
    @MethodSource("getMapTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forMapType(Message mapMessage) {
        String packageName = mapMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getMapSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(mapMessage, connectSchema);
        Struct expectedData = getMapTypeData(packageName);

        assertEquals(expectedData, actualData);
    }

    @ParameterizedTest
    @MethodSource("getTimeTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forTimeType(Message timeMessage) {
        String packageName = timeMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getTimeSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(timeMessage, connectSchema);
        Struct expectedData = getTimeTypeData(packageName);

        assertEquals(expectedData, actualData);
    }

    @ParameterizedTest
    @MethodSource("getDecimalTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forDecimalType(Message decimalMessage) {
        String packageName = decimalMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getDecimalSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(decimalMessage, connectSchema);
        Struct expectedData = getDecimalTypeData(packageName);

        assertEquals(expectedData, actualData);
    }

    @Test
    public void toConnectData_ForInvalidClassCasts_ThrowsDataException() {
        final Message message = getPrimitiveProtobufMessages().get(0);
        final Schema nonMatchingSchema =
            new SchemaBuilder(Schema.Type.STRUCT).field("i8", SchemaBuilder.string()).build();
        Exception ex = assertThrows(DataException.class,
            () -> PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(message, nonMatchingSchema));
        assertEquals(
            "Error converting value: \"2\" (Java Type: class java.lang.Integer, Protobuf type: INT32) to Connect type: STRING",
            ex.getMessage());
    }

    @Test
    public void toConnectData_ForMissingField_ThrowsDataException() {
        final Message message = getPrimitiveProtobufMessages().get(0);
        final Schema nonMatchingSchema =
            new SchemaBuilder(Schema.Type.STRUCT).field("invalidSchema", SchemaBuilder.string()).build();
        Exception ex = assertThrows(DataException.class,
            () -> PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(message, nonMatchingSchema));
        assertEquals(
            "Protobuf schema doesn't contain the connect field: invalidSchema",
            ex.getMessage());
    }

    @Test
    public void toConnectData_ForNullParams_ThrowsException() {
        final Message anyMessage = getPrimitiveProtobufMessages().get(0);
        final Schema anySchema = ToConnectTestDataGenerator.getPrimitiveSchema("any");
        assertThrows(IllegalArgumentException.class,
            () -> PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(null, anySchema));
        assertThrows(IllegalArgumentException.class,
            () -> PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(anyMessage, null));
    }
}