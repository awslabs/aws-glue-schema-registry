package com.amazonaws.services.schemaregistry.deserializers.protobuf;

import Foo.Contact;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufSerializer;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufTestCase;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufTestCaseReader;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.ComplexNestingSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.BasicSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.snake_case.SnakeCaseFile;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Basicsyntax3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.ConflictingNameOuterClass;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Foo1;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.HyphenAtedProtoFile;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.NestedConflictingClassNameOuterClass;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.ProtodevelaslProtoProtoProtodevelBar3_;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Special;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Unicode;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.snake_case.AnotherSnakeCaseProtoFile;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtobufWireFormatDecoderTest {
    private final GlueSchemaRegistryConfiguration configs = new GlueSchemaRegistryConfiguration("us-west-2");
    private final ProtobufSerializer protobufSerializer = new ProtobufSerializer(configs);
    private final ProtobufWireFormatDecoder decoder = new ProtobufWireFormatDecoder(new MessageIndexFinder());
    private final ProtobufTestCase basicTestCase = ProtobufTestCaseReader.getTestCaseByName("Basic.proto");
    private final Descriptors.FileDescriptor basicFileDescriptor = basicTestCase.getSchema();

    @Test
    public void testDecodeDynamicMessage_NullInputStream_ThrowsException() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> decoder.decode(null, basicFileDescriptor,
                ProtobufMessageType.DYNAMIC_MESSAGE));
        assertEquals("data is marked non-null but is null", ex.getMessage());
    }

    @Test
    public void testDecodeDynamicMessage_NullDescriptor_ThrowsException() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> decoder.decode(new byte[] {}, null, ProtobufMessageType.DYNAMIC_MESSAGE));
        assertEquals("descriptor is marked non-null but is null", ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("getDynamicMessageDecoderTestCases")
    public void testDecode_UnknownMessageTypeValidInputs_ToDynamicMessage_Succeeds(DynamicMessage dynamicMessage, ProtobufMessageType protobufMessageType) throws IOException {
        byte[] serializedBytes = protobufSerializer.serialize(dynamicMessage);
        DynamicMessage decoded = (DynamicMessage) decoder.decode(serializedBytes, dynamicMessage.getDescriptorForType().getFile(), protobufMessageType);
        assertArrayEquals(dynamicMessage.toByteArray(), decoded.toByteArray());
    }

    @Test
    public void testDecode_DynamicMessage_CorruptedMessageIndex_ThrowsException() {
        byte[] invalidData = "\uD83D\uDE0B".getBytes(StandardCharsets.UTF_8);
        Exception ex = assertThrows(InvalidProtocolBufferException.class,
            () -> decoder.decode(invalidData, basicFileDescriptor, ProtobufMessageType.DYNAMIC_MESSAGE));
        assertEquals("While parsing a protocol message, " +
            "the input ended unexpectedly in the middle of a field.  This could mean either that the input has " +
            "been truncated or that an embedded message misreported its own length.", ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("getPOJODecoderTestCases")
    public void testDecode_WhenMessagesArePassed_DeserializesThemIntoCorrectPOJOs(Message message, Class<?> expectedClass)
        throws IOException {
        byte[] data = protobufSerializer.serialize(message);
        Descriptors.FileDescriptor fileDescriptor = message.getDescriptorForType().getFile();

        Object decodedObject = decoder.decode(data, fileDescriptor, ProtobufMessageType.POJO);

        assertTrue(expectedClass.isInstance(decodedObject));
        assertEquals(message, decodedObject);
    }

    @Test
    public void testDecode_WhenPOJOClassIsNotFound_ThrowsRuntimeException()
        throws Descriptors.DescriptorValidationException {

        Message nonPOJOExistentMessage = ProtobufGenerator.createRuntimeCompiledRecord();
        byte[] nonExistentMessageBytes = protobufSerializer.serialize(nonPOJOExistentMessage);

        Exception ex = assertThrows(RuntimeException.class,
            () -> decoder.decode(nonExistentMessageBytes, nonPOJOExistentMessage.getDescriptorForType().getFile(), ProtobufMessageType.POJO));
        assertEquals("Error de-serializing data into Message class: foo.NonExistent$NonExistentSchema", ex.getMessage());

        Throwable rootCause = ex.getCause();
        assertEquals(ClassNotFoundException.class, rootCause.getClass());
        assertEquals("foo.NonExistent$NonExistentSchema", rootCause.getMessage());
    }

    private static Stream<Arguments> getPOJODecoderTestCases() {
        return Stream.of(
            Arguments.of(BASIC_SYNTAX2_MESSAGE, BasicSyntax2.Phone.class),
            Arguments.of(BASIC_SYNTAX3_MESSAGE, Basicsyntax3.Phone.class),
            Arguments.of(BASIC_REFERENCING_MESSAGE, Basic.Customer.class),
            Arguments.of(BASIC_REFERENCING_DYNAMIC_MESSAGE, Basic.Address.class),
            Arguments.of(JAVA_OUTER_CLASS_MESSAGE, Contact.Phone.class),
            Arguments.of(JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE,
                com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.Phone.class),
            Arguments.of(NESTING_MESSAGE_PROTO3, ComplexNestingSyntax3.A.B.C.X.D.F.M.class),
            Arguments.of(NESTING_MESSAGE_PROTO2, ComplexNestingSyntax2.O.A.class),
            Arguments.of(SNAKE_CASE_MESSAGE, SnakeCaseFile.snake_case_message.class),
            Arguments.of(ANOTHER_SNAKE_CASE_MESSAGE, AnotherSnakeCaseProtoFile.another_SnakeCase_.class),
            Arguments.of(DOLLAR_SYNTAX_3_MESSAGE, Foo1.Dollar.class),
            Arguments.of(HYPHEN_ATED_PROTO_FILE_MESSAGE, HyphenAtedProtoFile.hyphenated.class),
            Arguments.of(DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE, ProtodevelaslProtoProtoProtodevelBar3_.bar.class),
            Arguments.of(SPECIAL_CHARS_MESSAGE, Special.specialChars.class),
            Arguments.of(UNICODE_MESSAGE, Unicode.uni.class),
            Arguments.of(CONFLICTING_NAME_MESSAGE, ConflictingNameOuterClass.ConflictingName.class),
            Arguments.of(NESTED_CONFLICTING_NAME_MESSAGE,
                NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName.class),
            Arguments.of(NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
                com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.A.B.C.X.D.F.M.class)
        );
    }

    private static Stream<Arguments> getDynamicMessageDecoderTestCases() {
        return Stream.of(
            Arguments.of(BASIC_REFERENCING_DYNAMIC_MESSAGE, ProtobufMessageType.DYNAMIC_MESSAGE),
            Arguments.of(createDynamicNRecord(), ProtobufMessageType.DYNAMIC_MESSAGE),
            Arguments.of(createDynamicProtobufRecord(), ProtobufMessageType.DYNAMIC_MESSAGE),
            Arguments.of(createDynamicProtobufRecord(), null),
            Arguments.of(createDynamicProtobufRecord(), ProtobufMessageType.UNKNOWN)
        );
    }
}
