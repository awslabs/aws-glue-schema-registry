
package com.amazonaws.services.schemaregistry.serializers.protobuf;

import Foo.Contact;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.ComplexNestingSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.alltypes.AllTypesSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.BasicSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.ProtodevelaslProtoProtoProtodevelBar3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.snake_case.SnakeCaseFile;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.alltypes.AllTypes;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.alltypes.AnEnum;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.alltypes.AnotherTopLevelMessage;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.ConflictingNameOuterClass;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.NestedConflictingClassNameOuterClass;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Basicsyntax3;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Foo1;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.HyphenAtedProtoFile;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Unicode;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.A;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.snake_case.AnotherSnakeCaseProtoFile;
import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils;
import com.amazonaws.services.schemaregistry.utils.apicurio.syntax2.WellKnownTypesTestSyntax2;
import com.amazonaws.services.schemaregistry.utils.apicurio.syntax3.WellKnownTypesTestSyntax3;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.*;
import com.google.protobuf.Enum;
import com.google.type.*;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generates Protobuf objects to be used during testing
 */
public class ProtobufGenerator {
    public static List<Message> getAllPOJOMessages() {
        return Stream.of(
            BASIC_REFERENCING_MESSAGE,
            BASIC_SYNTAX2_MESSAGE,
            BASIC_SYNTAX3_MESSAGE,
            NESTING_MESSAGE_PROTO2,
            NESTING_MESSAGE_PROTO3,
            NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
            JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE,
            JAVA_OUTER_CLASS_MESSAGE,
            UNICODE_MESSAGE,
            NESTED_CONFLICTING_NAME_MESSAGE,
            ALL_TYPES_MESSAGE_SYNTAX3,
            ALL_TYPES_MESSAGE_SYNTAX2,
            WELL_KNOWN_TYPES_SYNTAX_2,
            WELL_KNOWN_TYPES_SYNTAX_3
        ).collect(Collectors.toList());
    }

    public static List<DynamicMessage> getAllDynamicMessages() {
        return Stream.of(
            BASIC_REFERENCING_DYNAMIC_MESSAGE,
            createDynamicProtobufRecord(),
            createDynamicNRecord(),
            createDynamicMessageFromPOJO(ALL_TYPES_MESSAGE_SYNTAX2),
            createDynamicMessageFromPOJO(ALL_TYPES_MESSAGE_SYNTAX3),
            createDynamicMessageFromPOJO(WELL_KNOWN_TYPES_SYNTAX_2),
            createDynamicMessageFromPOJO(WELL_KNOWN_TYPES_SYNTAX_3)
            //Add all types,
        ).collect(Collectors.toList());
    }

    @SneakyThrows
    private static DynamicMessage createDynamicMessageFromPOJO(Message pojo) {
        byte[] pojoBytes = pojo.toByteArray();
        return DynamicMessage
            .newBuilder(pojo.getDescriptorForType())
            .mergeFrom(pojoBytes)
            .build();
    }

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

    public static final A.B.C.X.D.F.M NESTING_MESSAGE_PROTO3_MULTIPLE_FILES =
        A.B.C.X.D.F.M.newBuilder().setChoice(
            A.B.C.X.D.F.M.K.L).build();

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

    public static final ProtodevelaslProtoProtoProtodevelBar3.bar DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE =
        ProtodevelaslProtoProtoProtodevelBar3.bar.newBuilder().build();

    public static final Unicode.uni UNICODE_MESSAGE =
        Unicode.uni.newBuilder().build();

    public static final ConflictingNameOuterClass.ConflictingName CONFLICTING_NAME_MESSAGE =
        ConflictingNameOuterClass.ConflictingName.newBuilder().build();

    public static final NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName NESTED_CONFLICTING_NAME_MESSAGE =
        NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName.newBuilder().build();

    public static final AllTypes ALL_TYPES_MESSAGE_SYNTAX3 =
        AllTypes.newBuilder()
            .setStringType("0asd29340932")
            .setByteType(ByteString.copyFrom(UNICODE_MESSAGE.toByteArray()))
            .setOneOfInt(93)
            .setOneOfMoney(Money.newBuilder().setCurrencyCode("INR").setUnits(4l).setNanos(2390).build())
            .addAllRepeatedString(ImmutableList.of("asd", "fgf"))
            .addAllRepeatedPackedInts(ImmutableList.of("1", "90", "34"))
            .setAnotherOneOfMoney(Money.newBuilder().setCurrencyCode("INR").setUnits(4l).setNanos(2390).build())
            .setOptionalSfixed32(1231)
            .setOptionalSfixed64(3092l)
            .setAnEnum2(AnEnum.ALPHA)
            .setUint64Type(1922l)
            .setInt32Type(91)
            .setSint32Type(-910)
            .setSint64Type(-9122)
            .setFixed32Type(19023)
            .setFixed64Type(123)
            .setNestedMessage1(AllTypes.NestedMessage1.newBuilder().setDoubleType(123123.1232).build())
            .putAComplexMap(90, AnotherTopLevelMessage.NestedMessage2.newBuilder().addAllATimestamp(
                ImmutableList.of(
                    Timestamp.newBuilder().setSeconds(123).setNanos(1).build(),
                    Timestamp.newBuilder().setNanos(0).build()
                )
            ).build())
            .setAnEnum1(AnEnum.BETA)
            .putAComplexMap(81, AnotherTopLevelMessage.NestedMessage2.newBuilder().addATimestamp(Timestamp.newBuilder().build()).build())
            .build();

    public static final AllTypesSyntax2.AllTypes ALL_TYPES_MESSAGE_SYNTAX2 =
        AllTypesSyntax2.AllTypes.newBuilder()
            .setStringType("0asd29340932")
            .setByteType(ByteString.copyFrom(UNICODE_MESSAGE.toByteArray()))
            .setOneOfInt(93)
            .setOneOfMoney(Money.newBuilder().setCurrencyCode("INR").setUnits(4l).setNanos(2390).build())
            .addAllRepeatedString(ImmutableList.of("asd", "fgf"))
            .addAllRepeatedPackedInts(ImmutableList.of("1", "90", "34"))
            .setAnotherOneOfMoney(Money.newBuilder().setCurrencyCode("INR").setUnits(4l).setNanos(2390).build())
            .setOptionalSfixed32(1231)
            .setOptionalSfixed64(3092l)
            .setAnEnum2(AllTypesSyntax2.AnEnum.BETA)
            .setUint64Type(1922l)
            .setInt32Type(91)
            .setSint32Type(-910)
            .setSint64Type(-9122)
            .setFixed32Type(19023)
            .setFixed64Type(123)
            .setNestedMessage1(AllTypesSyntax2.AllTypes.NestedMessage1.newBuilder().setDoubleType(123123.1232).build())
            .putAComplexMap(90,
                AllTypesSyntax2.AnotherTopLevelMessage.NestedMessage2.newBuilder()
                    .addAllATimestamp(
                    ImmutableList.of(
                        Timestamp.newBuilder().setSeconds(123).setNanos(1).build(),
                        Timestamp.newBuilder().setNanos(0).build()
                    )
                ).build())
            .build();

    public static final WellKnownTypesTestSyntax2.WellKnownTypesSyntax3 WELL_KNOWN_TYPES_SYNTAX_2 =
        WellKnownTypesTestSyntax2.WellKnownTypesSyntax3.newBuilder()
            .setA(101)
            .setFloating(0)
            .setF1(Timestamp.newBuilder().setSeconds(123).setNanos(1).build())
            .setF2(StringValue.newBuilder().setValue("stringValue").build())
            .setF4(Empty.newBuilder().build())
            .setF5(Duration.newBuilder().setNanos(5).setSeconds(10).build())
            .setF22(ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(2.2).build()).build())
            .setF27(Int32Value.newBuilder().setValue(27).build())
            .setF33(Struct.newBuilder().build())
            .setF35(UInt64Value.newBuilder().setValue(64).build())
            .setF37(Api.newBuilder().setName("newapi").build())
            .setF42(Enum.newBuilder().addEnumvalue(EnumValue.newBuilder().setName("enumValue").build()).build())
            .setF47(Method.newBuilder().setName("method").setRequestTypeUrl("sampleUrl").build())
            .setF48(Mixin.newBuilder().setName("mixin").build())
            .setF9(CalendarPeriod.DAY)
            .setF10(Color.newBuilder().setRed(100).setGreen(100).setBlue(100).build())
            .setF7(Date.newBuilder().setDay(1).setMonth(4).setYear(2022).build())
            .setF13(Fraction.newBuilder().setDenominator(100).setNumerator(9).build())
            .setF6(Money.newBuilder().setUnits(10).build())
            .setF14(Month.APRIL)
            .setF16(PostalAddress.newBuilder().setPostalCode("98121").build())
            .setF15(PhoneNumber.newBuilder().setE164Number("206").build())
            .build();

    public static final WellKnownTypesTestSyntax3.WellKnownTypesSyntax3 WELL_KNOWN_TYPES_SYNTAX_3 =
        WellKnownTypesTestSyntax3.WellKnownTypesSyntax3.newBuilder()
            .setA(101)
            .setFloating(0)
            .setF1(Timestamp.newBuilder().setSeconds(123).setNanos(1).build())
            .setF2(StringValue.newBuilder().setValue("stringValue").build())
            .setF4(Empty.newBuilder().build())
            .setF5(Duration.newBuilder().setNanos(5).setSeconds(10).build())
            .setF22(ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(2.2).build()).build())
            .setF27(Int32Value.newBuilder().setValue(27).build())
            .setF33(Struct.newBuilder().build())
            .setF35(UInt64Value.newBuilder().setValue(64).build())
            .setF37(Api.newBuilder().setName("newapi").build())
            .setF42(Enum.newBuilder().addEnumvalue(EnumValue.newBuilder().setName("enumValue").build()).build())
            .setF47(Method.newBuilder().setName("method").setRequestTypeUrl("sampleUrl").build())
            .setF48(Mixin.newBuilder().setName("mixin").build())
            .setF9(CalendarPeriod.DAY)
            .setF10(Color.newBuilder().setRed(100).setGreen(100).setBlue(100).build())
            .setF7(Date.newBuilder().setDay(1).setMonth(4).setYear(2022).build())
            .setF13(Fraction.newBuilder().setDenominator(100).setNumerator(9).build())
            .setF6(Money.newBuilder().setUnits(10).build())
            .setF14(Month.APRIL)
            .setF16(PostalAddress.newBuilder().setPostalCode("98121").build())
            .setF15(PhoneNumber.newBuilder().setE164Number("206").build())
            .build();
}