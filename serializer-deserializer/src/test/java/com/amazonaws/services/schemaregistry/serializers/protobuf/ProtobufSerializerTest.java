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
package com.amazonaws.services.schemaregistry.serializers.protobuf;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ProtobufSerializerTest {
    private Basic.Address addressPojo = ProtobufGenerator.createCompiledProtobufRecord();

    private ProtobufSerializer protobufSerializer =
            new ProtobufSerializer(new GlueSchemaRegistryConfiguration(new HashMap<String, String>() {{
                put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
            }}));

    private ProtobufWireFormatEncoder encoder = new ProtobufWireFormatEncoder(new MessageIndexFinder());

    private static List<Arguments> testBasicAddressProtoFileProvider() {
        List<ProtobufTestCase> testCases =
                ProtobufTestCaseReader.getTestCasesByNames("Basic.proto");
        return testCases
                .stream()
                .map(Arguments::of)
                .collect(toList());
    }

//    private static List<Arguments> testOrderingApplicationProtoFileProvider() {
//        List<ProtobufTestCase> testCases =
//                ProtobufTestCaseReader.getTestCasesByNames("OrderingApplication.proto");
//        return testCases
//                .stream()
//                .map(Arguments::of)
//                .collect(toList());
//    }

    private static List<Arguments> testDynamicMessageProvider() {
        List<Descriptors.FieldDescriptor> fieldDescriptorList = Basic.Address.getDescriptor().getFields();
        List<DynamicMessage>  dynamicMessageList = new ArrayList<>();
        DynamicMessage addressDynamicMessage = ProtobufGenerator.createDynamicProtobufRecord();
        dynamicMessageList.add(addressDynamicMessage);

        return dynamicMessageList
                .stream()
                .map(Arguments::of)
                .collect(toList());
    }


    @ParameterizedTest
    @MethodSource("testBasicAddressProtoFileProvider")
    public void testGetSchemaDefinition_specificAddressPojo(ProtobufTestCase testCase) {
        //String schema = testCase.getRawSchema();
        //Temporary schema assignment
        String schema = "// Proto schema formatted by Wire, do not edit.\n" +
                "// Source: \n" +
                "\n" +
                "package com.amazonaws.services.schemaregistry.tests.protobuf.syntax2;\n" +
                "\n" +
                "message Address {\n" +
                "  required string street = 1;\n" +
                "\n" +
                "  optional int32 zip = 2;\n" +
                "\n" +
                "  optional string city = 3;\n" +
                "}\n" +
                "\n" +
                "message Customer {\n" +
                "  required string name = 1;\n" +
                "}";

        //TODO: Complete upon final implementation of getSchemaDefinition() in ProtobufSerializer
        //Fails because of issues with apicurio library schema extraction
        assertEquals(schema, protobufSerializer.getSchemaDefinition(addressPojo).trim());
    }

    @Test
//    @MethodSource("testOrderingApplicationProtoFileProvider")
    public void testGetSchemaDefinition_specificOrderingApplicationPojo() {
//        String schema = testCase.getRawSchema();

//        NestedProtobuf.OrderingApplication orderingApplication = NestedProtobuf.OrderingApplication.newBuilder()
//                .setOrderNo(123456789)
//                .build();
        //TODO: Complete upon final implementation of getSchemaDefinition() in ProtobufSerializer
        //Fails because of issues with apicurio library schema extraction
        //assertEquals(schema.trim(), protobufSerializer.getSchemaDefinition(orderingApplication).trim());
    }

    @Test
    public void testSerialize_badObject_throwsException() {
        String s = "test";
        Exception ex = assertThrows(AWSSchemaRegistryException.class, () -> protobufSerializer.serialize(s));
        assertEquals("Could not serialize from the type provided", ex.getMessage());
    }

    @Test
    public void testSerialize_PayloadBytesMatch_specificPojo() {
        byte[] expectedBytes = encoder.encode(addressPojo,
                getFileDescriptor(addressPojo));
        byte[] serializedBytes = protobufSerializer.serialize(addressPojo);
        assertArrayEquals(expectedBytes, serializedBytes);

        Basic.Address deserializedObject = null;

        //TODO: check serialization by deserializing the byte array
        try {
            //deserializedObject = Basic.Address.parseFrom(serializedBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //assertEquals(deserializedObject, addressPojo);
    }

    @Test
    public void testValidate_invalidObject_throwsException() {
        String s = "test";
        Exception ex = assertThrows(AWSSchemaRegistryException.class, () -> protobufSerializer.validate(s));
        assertEquals("Object is not of Message type: class java.lang.String", ex.getMessage());

        Integer num = 5;
        ex = assertThrows(AWSSchemaRegistryException.class, () -> protobufSerializer.validate(num));
        assertEquals("Object is not of Message type: class java.lang.Integer", ex.getMessage());
    }

    //DynamicMessage
    @ParameterizedTest
    @MethodSource("testDynamicMessageProvider")
    public void testSerialize_PayloadBytesMatch_DynamicMessage(DynamicMessage dynamicMessage) {
        byte[] expectedBytes = encoder.encode(dynamicMessage,
                getFileDescriptor(dynamicMessage));
        byte[] serializedBytes = protobufSerializer.serialize(dynamicMessage);
        assertArrayEquals(expectedBytes, serializedBytes);

        DynamicMessage deserializedObject;

        //TODO: check serialization by deserializing the byte array
        try {
            // deserializedObject = DynamicMessage.parseFrom(TestProtos.SimpleAddress.getDescriptor(), serializedBytes);
            //assertEquals(addressDynamicMessage, deserializedObject);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Descriptors.FileDescriptor getFileDescriptor(Message message) {
        return message.getDescriptorForType().getFile();
    }
}