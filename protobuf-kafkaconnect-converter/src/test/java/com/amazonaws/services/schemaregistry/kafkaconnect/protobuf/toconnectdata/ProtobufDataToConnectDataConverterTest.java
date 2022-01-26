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

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveProtobufMessages;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufDataToConnectDataConverterTest {
    private static final ProtobufDataToConnectDataConverter PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER =
        new ProtobufDataToConnectDataConverter();

    private static Stream<Arguments> getPrimitiveTestCases() {
        return getPrimitiveProtobufMessages().stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("getPrimitiveTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forPrimitiveTypes(Message primitiveMessage) {
        String packageName = primitiveMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = ToConnectTestDataGenerator.getPrimitiveSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(primitiveMessage, connectSchema);
        Struct expectedData = ToConnectTestDataGenerator.getPrimitiveTypesData(packageName);

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