/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates.
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

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getAllTypesProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getAllTypesSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArrayProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArraySchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArrayTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getOneofProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getOneofSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getOneofTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveTypesData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getAllTypesData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getStructProtobufMessages;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getStructSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getStructTypeData;
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
  
    private static Stream<Arguments> getStructTestCases() {
        return getStructProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getOneofTestCases() {
        return getOneofProtobufMessages().stream().map(Arguments::of);
    }

    private static Stream<Arguments> getAllTypesTestCases() {
        return getAllTypesProtobufMessages().stream().map(Arguments::of);
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

    @ParameterizedTest
    @MethodSource("getStructTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forStructType(Message nestedMessage) {
        String packageName = nestedMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getStructSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(nestedMessage, connectSchema);
        Struct expectedData = getStructTypeData(packageName);

        assertEquals(expectedData, actualData);
    }

    @ParameterizedTest
    @MethodSource("getOneofTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forOneofType(Message oneofMessage) {
        String packageName = oneofMessage.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getOneofSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(oneofMessage, connectSchema);
        Struct expectedData = getOneofTypeData(packageName);

        assertEquals(expectedData, actualData);
    }

    @ParameterizedTest
    @MethodSource("getAllTypesTestCases")
    public void toConnectData_convertsProtobufMessageToConnect_forAllTypes(Message message) {
        String packageName = message.getDescriptorForType().getFile().getPackage();
        final Schema connectSchema = getAllTypesSchema(packageName);
        Object actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(message, connectSchema);
        Struct expectedData = getAllTypesData(packageName);

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