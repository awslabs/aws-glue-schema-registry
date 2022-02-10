package com.amazonaws.services.schemaregistry.deserializers.protobuf;

import Foo.Contact;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.ComplexNestingSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.BasicSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.ProtodevelaslProtoProtoProtodevelBar3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.snake_case.SnakeCaseFile;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Basicsyntax3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.ConflictingNameOuterClass;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Foo1;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.HyphenAtedProtoFile;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.NestedConflictingClassNameOuterClass;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Unicode;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.snake_case.AnotherSnakeCaseProtoFile;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufClassNameTest {

    @Test
    public void testFrom_OnNullDescriptor_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> ProtobufClassName.from(null));
    }

    @ParameterizedTest
    @MethodSource("getPOJODecoderTestCases")
    public void testFrom_ConvertsFileDescriptorToClassNames_ForAllCases(Message message, Class<?> expectedClassName) {
        Descriptors.Descriptor messageDescriptor = message.getDescriptorForType();

        String actualClassName = ProtobufClassName.from(messageDescriptor);
        assertEquals(expectedClassName.getName(), actualClassName);
        assertDoesNotThrow(() -> Class.forName(actualClassName));
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
            Arguments.of(DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE, ProtodevelaslProtoProtoProtodevelBar3.bar.class),
            Arguments.of(UNICODE_MESSAGE, Unicode.uni.class),
            Arguments.of(CONFLICTING_NAME_MESSAGE, ConflictingNameOuterClass.ConflictingName.class),
            Arguments.of(NESTED_CONFLICTING_NAME_MESSAGE,
                NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName.class),
            Arguments.of(NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
                com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.A.B.C.X.D.F.M.class)
        );
    }
}