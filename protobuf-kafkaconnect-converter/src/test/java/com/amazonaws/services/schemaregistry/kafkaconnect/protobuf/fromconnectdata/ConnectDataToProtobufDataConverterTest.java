package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConnectDataToProtobufDataConverterTest {
    private final ConnectDataToProtobufDataConverter connectDataToProtobufDataConverter =
        new ConnectDataToProtobufDataConverter();

    private static Stream<Arguments> getInvalidSchemaTypesForConverters() {
        return Stream.of(
            Arguments.of(
                new PrimitiveDataConverter(),
                SchemaBuilder.struct().build()
            )
        );
    }

    @Test
    public void convert_ForPrimitiveTypes_ConvertsSuccessfully() {
        final DynamicMessage primitiveMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage();
        final Descriptors.FileDescriptor fileDescriptor = primitiveMessage.getDescriptorForType().getFile();
        final Schema primitiveSchema = ToProtobufTestDataGenerator.getPrimitiveSchema("primitiveProtobufSchema");
        final Message actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, primitiveSchema,
            ToProtobufTestDataGenerator.getPrimitiveTypesData());

        assertEquals(primitiveMessage, actualMessage);
    }

    @Test
    public void convert_ForEnumTypes_ConvertsSuccessfully() {
        final DynamicMessage enumMessage = ToProtobufTestDataGenerator.getProtobufEnumMessage();
        final Descriptors.FileDescriptor fileDescriptor = enumMessage.getDescriptorForType().getFile();
        final Schema enumSchema = ToProtobufTestDataGenerator.getEnumSchema("enumProtobufSchema");
        final Message actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, enumSchema,
            ToProtobufTestDataGenerator.getEnumTypeData());

        assertEquals(enumMessage, actualMessage);
    }

    @Test
    public void convert_ForArrayType_ConvertsSuccessfully() {
        // TODO add test case for repeated Message/Enum and other complex types
        final DynamicMessage arrayMessage = ToProtobufTestDataGenerator.getProtobufArrayMessage();
        final Descriptors.FileDescriptor fileDescriptor = arrayMessage.getDescriptorForType().getFile();
        final Schema arraySchema = ToProtobufTestDataGenerator.getArraySchema("arrayProtobufSchema");
        final Message actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, arraySchema,
            ToProtobufTestDataGenerator.getArrayTypeData());

        assertEquals(arrayMessage, actualMessage);
    }

    @Test
    public void convert_ForMapType_ConvertsSuccessfully() {
        final DynamicMessage mapMessage = ToProtobufTestDataGenerator.getProtobufMapMessage();
        final Descriptors.FileDescriptor fileDescriptor = mapMessage.getDescriptorForType().getFile();
        final Schema mapSchema = ToProtobufTestDataGenerator.getMapSchema("mapProtobufSchema");
        final Message actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, mapSchema,
            ToProtobufTestDataGenerator.getMapTypeData());

        assertEquals(mapMessage, actualMessage);
    }

    @Test
    public void convert_ForTimeType_ConvertsSuccessfully() {
        final DynamicMessage timeMessage = ToProtobufTestDataGenerator.getProtobufTimeMessage();
        final Descriptors.FileDescriptor fileDescriptor = timeMessage.getDescriptorForType().getFile();
        final Schema timeSchema = ToProtobufTestDataGenerator.getTimeSchema("timeProtobufSchema");
        final Message actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, timeSchema,
                ToProtobufTestDataGenerator.getTimeTypeData());

        assertEquals(timeMessage, actualMessage);
    }

    @Test
    public void convert_ForDecimalType_ConvertsSuccessfully() {
        final DynamicMessage decimalMessage = ToProtobufTestDataGenerator.getProtobufDecimalMessage();
        final Descriptors.FileDescriptor fileDescriptor = decimalMessage.getDescriptorForType().getFile();
        final Schema decimalSchema = ToProtobufTestDataGenerator.getDecimalSchema("decimalProtobufSchema");
        final Message actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, decimalSchema,
                ToProtobufTestDataGenerator.getDecimalTypeData());

        assertEquals(decimalMessage, actualMessage);
    }
  
    @Test
    public void convert_ForNestedType_ConvertsSuccessfully() {
        final DynamicMessage nestedMessage = ToProtobufTestDataGenerator.getProtobufNestedMessage("NestedType");
        final Descriptors.FileDescriptor fileDescriptor = nestedMessage.getDescriptorForType().getFile();
        final Schema nestedSchema = ToProtobufTestDataGenerator.getStructSchema("NestedType");
        final Message actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, nestedSchema,
                ToProtobufTestDataGenerator.getStructTypeData("NestedType"));

        assertEquals(nestedMessage, actualMessage);
    }

    @Test
    public void convert_ForOneofType_ConvertsSuccessfully() {
        final DynamicMessage oneofMessage = ToProtobufTestDataGenerator.getProtobufOneofMessage();
        final Descriptors.FileDescriptor fileDescriptor = oneofMessage.getDescriptorForType().getFile();
        final Schema oneofSchema = ToProtobufTestDataGenerator.getOneofSchema("oneofProtobufSchema");
        final Message actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, oneofSchema,
                ToProtobufTestDataGenerator.getOneofTypeData());

        assertEquals(oneofMessage, actualMessage);
    }

    @Test
    public void convert_ForNullValues_ThrowsException() {
        final DynamicMessage primitiveMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage();
        final Schema primitiveSchema = ToProtobufTestDataGenerator.getPrimitiveSchema("primitiveProtobufSchema");
        final Descriptors.FileDescriptor fileDescriptor = primitiveMessage.getDescriptorForType().getFile();

        assertThrows(IllegalArgumentException.class,
            () -> connectDataToProtobufDataConverter.convert(null, primitiveSchema,
            ToProtobufTestDataGenerator.getPrimitiveTypesData()));

        assertThrows(IllegalArgumentException.class,
            () -> connectDataToProtobufDataConverter.convert(fileDescriptor, null,
                ToProtobufTestDataGenerator.getPrimitiveTypesData()));

        assertThrows(IllegalArgumentException.class,
            () -> connectDataToProtobufDataConverter.convert(fileDescriptor, primitiveSchema,
                null));
    }

    @Test
    public void convert_WhenSchemaIsNotOptionalForNullValues_ThrowsException() {
        final DynamicMessage primitiveMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage();
        final Schema nonOptionalSchema = SchemaBuilder.struct().name("primitiveProtobufSchema")
                .field("nonOpt", SchemaBuilder.int64()).build();
        final Field nonOptionalField = new Field("nonOpt", 0, SchemaBuilder.int64().optional());
        final Descriptors.FileDescriptor fileDescriptor = primitiveMessage.getDescriptorForType().getFile();
        final Struct value = new Struct(nonOptionalSchema).put(nonOptionalField, null);

        assertThrows(DataException.class,
            () -> connectDataToProtobufDataConverter.convert(fileDescriptor, nonOptionalSchema, value));
    }

    @Test
    public void convert_WhenValueCannotBeCasted_ThrowsException() {
        final DynamicMessage primitiveMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage();
        final Schema nonOptionalSchema = SchemaBuilder.struct().name("primitiveProtobufSchema")
                .field("nonOpt", SchemaBuilder.int32()).build();
        final Field nonOptionalField = new Field("nonOpt", 0, SchemaBuilder.string());
        final Descriptors.FileDescriptor fileDescriptor = primitiveMessage.getDescriptorForType().getFile();
        final Struct value = new Struct(nonOptionalSchema).put(nonOptionalField, "some-string");

        assertThrows(DataException.class,
            () -> connectDataToProtobufDataConverter.convert(fileDescriptor, nonOptionalSchema, value));
    }

    @ParameterizedTest
    @MethodSource("getInvalidSchemaTypesForConverters")
    public void convert_ThrowsException_WhenIncorrectSchemaTypeIsSentToConverter(DataConverter dataConverter, Schema schema) {
        final DynamicMessage anyMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage();
        final Descriptors.FieldDescriptor anyFieldDescriptor = anyMessage.getDescriptorForType().getFields().get(0);
        assertThrows(
            DataException.class,
            () -> dataConverter.toProtobufData(schema, anyMessage, anyFieldDescriptor, anyMessage.toBuilder())
        );
    }
}