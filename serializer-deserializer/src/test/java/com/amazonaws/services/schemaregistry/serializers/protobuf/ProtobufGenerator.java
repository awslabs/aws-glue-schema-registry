
package com.amazonaws.services.schemaregistry.serializers.protobuf;

import Foo.Contact;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.ComplexNestingSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.BasicSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.snake_case.SnakeCaseFile;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.ConflictingNameOuterClass;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.NestedConflictingClassNameOuterClass;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.ProtodevelaslProtoProtoProtodevelBar3_;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Basicsyntax3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Foo1;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.HyphenAtedProtoFile;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Special;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Unicode;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.snake_case.AnotherSnakeCaseProtoFile;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;

import java.util.List;
import java.util.Optional;

/**
 * Generates Protobuf objects to be used during testing
 */
public class ProtobufGenerator {
    public static Basic.Address createCompiledProtobufRecord() {
        return Basic.Address.newBuilder()
                .setStreet("410 Terry Ave. North")
                .setCity("Seattle")
                .setZip(98109)
                .build();
    }

    public static DynamicMessage createDynamicProtobufRecord() {
        List<Descriptors.FieldDescriptor> fieldDescriptorList = Basic.Address.getDescriptor().getFields();
        return DynamicMessage.newBuilder(Basic.Address.getDescriptor())
                .setField(fieldDescriptorList.get(0), "5432 82nd St")
                .setField(fieldDescriptorList.get(1), 123456)
                .setField(fieldDescriptorList.get(2),"Seattle")
                .build();
    }

    public static DynamicMessage createDynamicNRecord() {
        return DynamicMessage.newBuilder(ComplexNestingSyntax3.N.getDescriptor())
                .setField(ComplexNestingSyntax3.N.getDescriptor().findFieldByName("A"), 100)
                .build();
    }

    /**
     * Creates a Message from a dynamic schema that is only compiled during runtime.
     * There are no POJOs pre-compiled for this schema.
     */
    public static Message createRuntimeCompiledRecord() throws Descriptors.DescriptorValidationException {
        String nonPojoExistentSchemaDefinition =
            "package foo; message NonExistentSchema { optional string a = 1; }";

        Descriptors.FileDescriptor fileDescriptor = FileDescriptorUtils
            .protoFileToFileDescriptor(nonPojoExistentSchemaDefinition, "NonExistent.proto",
                Optional.of("foo"));

        //Create a message using above fileDescriptor
        return DynamicMessage.newBuilder(fileDescriptor.findMessageTypeByName("NonExistentSchema")).build();
    }

    private static final String NAME = "Foo";
    public static final Basic.Customer
        BASIC_REFERENCING_MESSAGE = Basic.Customer.newBuilder().setName(NAME).build();

    public static final DynamicMessage
        BASIC_REFERENCING_DYNAMIC_MESSAGE = DynamicMessage.newBuilder(Basic.Address.getDescriptor())
        .setField(Basic.Address.getDescriptor().findFieldByName("street"), NAME).build();

    public static final BasicSyntax2.Phone
        BASIC_SYNTAX2_MESSAGE = BasicSyntax2.Phone.newBuilder().setModel(NAME).build();

    public static final Basicsyntax3.Phone
        BASIC_SYNTAX3_MESSAGE = Basicsyntax3.Phone.newBuilder().setModel(NAME).build();

    public static final ComplexNestingSyntax3.A.B.C.X.D.F.M
        NESTING_MESSAGE_PROTO3 = ComplexNestingSyntax3.A.B.C.X.D.F.M.newBuilder().setChoice(
        ComplexNestingSyntax3.A.B.C.X.D.F.M.K.L).build();

    public static final ComplexNestingSyntax2.O.A
        NESTING_MESSAGE_PROTO2 = ComplexNestingSyntax2.O.A.newBuilder().addB("12312").build();

    public static final Object NESTING_MESSAGE_PROTO3_MULTIPLE_FILES =
        com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.A.B.C.X.D.F.M.newBuilder().setChoice(
            com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.A.B.C.X.D.F.M.K.L).build();

    public static final com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.Phone
        JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE =
        com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.Phone.newBuilder().build();

    public static final Contact.Phone JAVA_OUTER_CLASS_MESSAGE = Contact.Phone.newBuilder().build();

    public static final SnakeCaseFile.snake_case_message SNAKE_CASE_MESSAGE =
        SnakeCaseFile.snake_case_message.newBuilder().build();

    public static final AnotherSnakeCaseProtoFile.another_SnakeCase_
        ANOTHER_SNAKE_CASE_MESSAGE = AnotherSnakeCaseProtoFile.another_SnakeCase_.newBuilder().build();

    public static final Foo1.Dollar DOLLAR_SYNTAX_3_MESSAGE = Foo1.Dollar.newBuilder().build();
    public static final HyphenAtedProtoFile.hyphenated HYPHEN_ATED_PROTO_FILE_MESSAGE =
        HyphenAtedProtoFile.hyphenated.newBuilder().build();

    public static final ProtodevelaslProtoProtoProtodevelBar3_.bar DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE =
        ProtodevelaslProtoProtoProtodevelBar3_.bar.newBuilder().build();

    public static final Special.specialChars SPECIAL_CHARS_MESSAGE =
        Special.specialChars.newBuilder().build();

    public static final Unicode.uni UNICODE_MESSAGE =
        Unicode.uni.newBuilder().build();

    public static final ConflictingNameOuterClass.ConflictingName CONFLICTING_NAME_MESSAGE =
        ConflictingNameOuterClass.ConflictingName.newBuilder().build();

    public static final NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName NESTED_CONFLICTING_NAME_MESSAGE =
        NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName.newBuilder().build();
}