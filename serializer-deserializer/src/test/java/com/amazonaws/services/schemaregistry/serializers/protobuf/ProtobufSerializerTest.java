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
import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufWireFormatDecoder;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.ANOTHER_SNAKE_CASE_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.BASIC_REFERENCING_DYNAMIC_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.BASIC_REFERENCING_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.BASIC_SYNTAX2_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.BASIC_SYNTAX3_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.CONFLICTING_NAME_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.DOLLAR_SYNTAX_3_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.HYPHEN_ATED_PROTO_FILE_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.JAVA_OUTER_CLASS_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.NESTED_CONFLICTING_NAME_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.NESTING_MESSAGE_PROTO2;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.NESTING_MESSAGE_PROTO3;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.NESTING_MESSAGE_PROTO3_MULTIPLE_FILES;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.SNAKE_CASE_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.UNICODE_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufTestCaseReader.getTestCaseByName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufSerializerTest {
    private ProtobufSerializer protobufSerializer =
            new ProtobufSerializer(new GlueSchemaRegistryConfiguration(new HashMap<String, String>() {{
                put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
            }}));
    private ProtobufWireFormatDecoder protobufWireFormatDecoder = new ProtobufWireFormatDecoder(new MessageIndexFinder());

    private static Stream<Arguments> testMessageProvider() {
        return Stream.of(
            Arguments.of(BASIC_SYNTAX2_MESSAGE),
            Arguments.of(BASIC_SYNTAX3_MESSAGE),
            Arguments.of(BASIC_REFERENCING_MESSAGE),
            Arguments.of(BASIC_REFERENCING_DYNAMIC_MESSAGE),
            Arguments.of(JAVA_OUTER_CLASS_MESSAGE),
            Arguments.of(JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE),
            Arguments.of(NESTING_MESSAGE_PROTO3),
            Arguments.of(NESTING_MESSAGE_PROTO2),
            Arguments.of(SNAKE_CASE_MESSAGE),
            Arguments.of(ANOTHER_SNAKE_CASE_MESSAGE),
            Arguments.of(DOLLAR_SYNTAX_3_MESSAGE),
            Arguments.of(HYPHEN_ATED_PROTO_FILE_MESSAGE),
            Arguments.of(DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE),
            Arguments.of(UNICODE_MESSAGE),
            Arguments.of(CONFLICTING_NAME_MESSAGE),
            Arguments.of(NESTED_CONFLICTING_NAME_MESSAGE),
            Arguments.of(NESTING_MESSAGE_PROTO3_MULTIPLE_FILES),
            Arguments.of(ProtobufGenerator.createDynamicNRecord()),
            Arguments.of(ProtobufGenerator.createDynamicProtobufRecord()),
            Arguments.of(ProtobufGenerator.createCompiledProtobufRecord()),
            Arguments.of(ProtobufGenerator.createCompiledProtobufRecord())
        );
    }

    private static Stream<Arguments> testMessageProviderForCaching() {
        return Stream.of(
            Arguments.of(BASIC_SYNTAX2_MESSAGE),
            Arguments.of(BASIC_SYNTAX3_MESSAGE),
            Arguments.of(ProtobufGenerator.createDynamicNRecord()),
            Arguments.of(ProtobufGenerator.createDynamicNRecord()),
            Arguments.of(BASIC_SYNTAX3_MESSAGE)
        );
    }

    private static Stream<Arguments> testProtobufSchemaDefinitionProvider() {
        return Stream.of(
            Arguments.of(
                NESTING_MESSAGE_PROTO2, getTestCaseByName("ComplexNestingSyntax2.proto").getRawSchema(), "ComplexNestingSyntax2.proto"
            ),
            Arguments.of(
                NESTING_MESSAGE_PROTO3, getTestCaseByName("ComplexNestingSyntax3.proto").getRawSchema(), "ComplexNestingSyntax3.proto"
            ),
            Arguments.of(
                BASIC_REFERENCING_DYNAMIC_MESSAGE, getTestCaseByName("Basic.proto").getRawSchema(), "Basic.proto"
            ),
            Arguments.of(
                BASIC_SYNTAX3_MESSAGE, getTestCaseByName("basicsyntax3.proto").getRawSchema(), "basicsyntax3"
            ),
            Arguments.of(
                BASIC_SYNTAX2_MESSAGE, getTestCaseByName("basicSyntax2.proto").getRawSchema(), "basicSyntax2"
            ),
            Arguments.of(
                DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE, getTestCaseByName(".protodevelasl.proto.proto.protodevel$---$$.bar.3.proto").getRawSchema(), ".protodevelasl.proto.proto.protodevel$---$$.bar.3"
            ),
            Arguments.of(
                UNICODE_MESSAGE, getTestCaseByName("◉◉◉unicode⏩.proto").getRawSchema(), "◉◉◉unicode⏩.proto"
            ),
            Arguments.of(
                DOLLAR_SYNTAX_3_MESSAGE, getTestCaseByName("foo$$$1.proto").getRawSchema(), "foo$$$1.proto"
            ),
            Arguments.of(
                ProtobufGenerator.createDynamicProtobufRecord(), getTestCaseByName("Basic.proto").getRawSchema(), "Basic.proto"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("testMessageProvider")
    public void testSerialize_ProducesValidDeserializableBytes_ForAllTypesOfMessages(Message message) throws IOException {
        byte[] serializedBytes = protobufSerializer.serialize(message);

        DynamicMessage deserializedDynamicMessage =
            (DynamicMessage) protobufWireFormatDecoder.decode(serializedBytes, getFileDescriptor(message), ProtobufMessageType.DYNAMIC_MESSAGE);
        Message deserializedPojoMessage =
            (Message) protobufWireFormatDecoder.decode(serializedBytes, getFileDescriptor(message), ProtobufMessageType.POJO);

        //Assert that the message can de-serialized back into original Message.
        assertEquals(message, deserializedDynamicMessage);
        assertEquals(message, deserializedPojoMessage);
    }

    @ParameterizedTest
    @MethodSource("testProtobufSchemaDefinitionProvider")
    public void testGetSchemaDefinition_GeneratesValidSchemaDefinition_ForAllTypesOfMessages(Message message, String schemaDefinition, String schemaName)
        throws Descriptors.DescriptorValidationException {
        String parsedSchemaDefinition = protobufSerializer.getSchemaDefinition(message);
        assertFalse(parsedSchemaDefinition.contains("// Proto schema formatted by Wire, do not edit.\n// Source: \n\n"));
        String packageName = ProtoParser.Companion.parse(Location.get(""), schemaDefinition).getPackageName();

        DescriptorProtos.FileDescriptorProto expectedFileDescriptorProto =
            FileDescriptorUtils.protoFileToFileDescriptor(schemaDefinition, schemaName, Optional.ofNullable(packageName)).toProto();

        DescriptorProtos.FileDescriptorProto parsedFileDescriptorProto =
            FileDescriptorUtils.protoFileToFileDescriptor(parsedSchemaDefinition, schemaName, Optional.ofNullable(packageName)).toProto();
        assertEquals(expectedFileDescriptorProto, parsedFileDescriptorProto);
    }

    @Test
    public void testValidate_invalidObject_throwsException() {
        String s = "test";
        Exception ex = assertThrows(AWSSchemaRegistryException.class, () -> protobufSerializer.validate(s));
        assertEquals("Object is not of Message type: class java.lang.String", ex.getMessage());

        Integer num = 5;
        ex = assertThrows(AWSSchemaRegistryException.class, () -> protobufSerializer.validate(num));
        assertEquals("Object is not of Message type: class java.lang.Integer", ex.getMessage());

        ex = assertThrows(IllegalArgumentException.class, () -> protobufSerializer.validate(null));
        assertEquals("object is marked non-null but is null", ex.getMessage());
    }

    @Test
    public void testSerialize_CachesGeneratedSchema_ForSameArguments() {
        //Get schema definition for repeated messages of different types.
        testMessageProviderForCaching()
            .map(Arguments::get)
            .map(objects -> objects[0])
            .forEach(protobufSerializer::getSchemaDefinition);

        assertEquals(3, protobufSerializer.schemaGeneratorCache.size());
    }

    private Descriptors.FileDescriptor getFileDescriptor(Message message) {
        return message.getDescriptorForType().getFile();
    }
}