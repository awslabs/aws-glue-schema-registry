// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax3.Alltypes;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Google.Type;
using A = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax3.Multiplefiles.A;
using AllTypesSyntax2 = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2.Alltypes;
using AnotherSnakeCaseProtoFile = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax3.SnakeCase;
using Basicsyntax2 = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2.Basic;
using Basicsyntax3 = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax3.Basic;
using ComplexNestingSyntax2 = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2;
using ComplexNestingSyntax3 = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax3;
using Multiplefiles = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax3.Multiplefiles;
using SnakeCaseFileSyntax2 = Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2.SnakeCase;
using WellKnownTypesTestSyntax2 = Com.Amazonaws.Services.Schemaregistry.Utils.Apicurio.Syntax2;
using WellKnownTypesTestSyntax3 = Com.Amazonaws.Services.Schemaregistry.Utils.Apicurio.Syntax3;

// ReSharper disable InconsistentNaming
namespace AWSGsrSerDe.Tests.utils
{
    public static class ProtobufGenerator
    {
        private const string Name = "Customer Name";

        public static readonly ComplexNestingSyntax2.Customer BASIC_REFERENCING_MESSAGE =
            new ComplexNestingSyntax2.Customer
            {
                Name = Name,
            };

        public static readonly Basicsyntax2.Phone BASIC_SYNTAX2_MESSAGE = new Basicsyntax2.Phone
        {
            Name = Name,
        };

        public static readonly Basicsyntax3.Phone BASIC_SYNTAX3_MESSAGE = new Basicsyntax3.Phone
        {
            Name = Name,
        };

        public static readonly ComplexNestingSyntax3.A.Types.B.Types.C.Types.X.Types.D.Types.F.Types.M
            NESTING_MESSAGE_PROTO3 =
                new ComplexNestingSyntax3.A.Types.B.Types.C.Types.X.Types.D.Types.F.Types.M
                {
                    Choice = ComplexNestingSyntax3.A.Types.B.Types.C.Types.X.Types.D.Types.F.Types.M.Types.K.L,
                };

        public static readonly ComplexNestingSyntax2.O.Types.A NESTING_MESSAGE_PROTO2 =
            new ComplexNestingSyntax2.O.Types.A
            {
                B = { "12312", },
            };

        public static readonly A.Types.B.Types.C.Types.X.Types.D.Types.F.Types.M NESTING_MESSAGE_PROTO3_MULTIPLE_FILES =
            new A.Types.B.Types.C.Types.X.Types.D.Types.F.Types.M
            {
                Choice = A.Types.B.Types.C.Types.X.Types.D.Types.F.Types.M.Types.K.L,
            };

        public static readonly Multiplefiles.Phone DOTNET_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE =
            new Multiplefiles.Phone();

        public static readonly SnakeCaseFileSyntax2.snake_case_message SNAKE_CASE_MESSAGE =
            new SnakeCaseFileSyntax2.snake_case_message();

        public static readonly AnotherSnakeCaseProtoFile.another_SnakeCase_ ANOTHER_SNAKE_CASE_MESSAGE =
            new AnotherSnakeCaseProtoFile.another_SnakeCase_();

        public static readonly Basicsyntax3.Dollar DOLLAR_SYNTAX_3_MESSAGE = new Basicsyntax3.Dollar();

        public static readonly Basicsyntax3.hyphenated HYPHEN_ATED_PROTO_FILE_MESSAGE = new Basicsyntax3.hyphenated();

        public static readonly Basicsyntax2.bar DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE = new Basicsyntax2.bar();

        public static readonly Basicsyntax3.uni UNICODE_MESSAGE = new Basicsyntax3.uni();

        public static readonly Basicsyntax3.ConflictingName CONFLICTING_NAME_MESSAGE =
            new Basicsyntax3.ConflictingName();

        public static readonly Basicsyntax3.Parent.Types.NestedConflictingClassName NESTED_CONFLICTING_NAME_MESSAGE =
            new Basicsyntax3.Parent.Types.NestedConflictingClassName();

        public static readonly AllTypes ALL_TYPES_MESSAGE_SYNTAX3 = new AllTypes
        {
            StringType = "0asd29340932",
            ByteType = ByteString.CopyFrom(UNICODE_MESSAGE.ToByteArray()),
            OneOfInt = 93,
            OneOfMoney = new Money
            {
                CurrencyCode = "INR",
                Units = 4L,
                Nanos = 2390,
            },
            RepeatedString = { "asd", "fgf" },
            RepeatedPackedInts = { "1", "90", "34" },
            AnotherOneOfMoney = new Money
            {
                CurrencyCode = "INR",
                Units = 4L,
                Nanos = 2390,
            },
            OptionalSfixed32 = 1231,
            OptionalSfixed64 = 3092L,
            AnEnum2 = AnEnum.Alpha,
            Uint64Type = 1922L,
            Int32Type = 91,
            Sint32Type = -910,
            Sint64Type = -9122,
            Fixed32Type = 19023,
            Fixed64Type = 123,
            NestedMessage1 = new AllTypes.Types.NestedMessage1 { DoubleType = 123123.1232 },
            AComplexMap =
            {
                {
                    90, new AnotherTopLevelMessage.Types.NestedMessage2
                    {
                        ATimestamp =
                        {
                            new Timestamp
                            {
                                Seconds = 123,
                                Nanos = 1,
                            },
                            new Timestamp
                            {
                                Nanos = 0,
                            },
                        },
                    }
                },
                {
                    81, new AnotherTopLevelMessage.Types.NestedMessage2
                    {
                        ATimestamp = { new Timestamp() },
                    }
                },
            },
            AnEnum1 = AnEnum.Beta,
        };

        public static readonly AllTypesSyntax2.AllTypes ALL_TYPES_MESSAGE_SYNTAX2 = new AllTypesSyntax2.AllTypes
        {
            StringType = "0asd29340932",
            ByteType = ByteString.CopyFrom(UNICODE_MESSAGE.ToByteArray()),
            OneOfInt = 93,
            OneOfMoney = new Money
            {
                CurrencyCode = "INR",
                Units = 4L,
                Nanos = 2390,
            },
            RepeatedString = { "asd", "fgf" },
            RepeatedPackedInts = { "1", "90", "34" },
            AnotherOneOfMoney = new Money
            {
                CurrencyCode = "INR",
                Units = 4L,
                Nanos = 2390,
            },
            OptionalSfixed32 = 1231,
            OptionalSfixed64 = 3092L,
            AnEnum2 = AllTypesSyntax2.AnEnum.Beta,
            Uint64Type = 1922L,
            Int32Type = 91,
            Sint32Type = -910,
            Sint64Type = -9122,
            Fixed32Type = 19023,
            Fixed64Type = 123,
            NestedMessage1 = new AllTypesSyntax2.AllTypes.Types.NestedMessage1() { DoubleType = 123123.1232 },
            AComplexMap =
            {
                {
                    90, new AllTypesSyntax2.AnotherTopLevelMessage.Types.NestedMessage2()
                    {
                        ATimestamp =
                        {
                            new Timestamp
                            {
                                Seconds = 123,
                                Nanos = 1,
                            },
                            new Timestamp
                            {
                                Nanos = 0,
                            },
                        },
                    }
                },
            },
        };

        public static readonly WellKnownTypesTestSyntax2.WellKnownTypesSyntax3 WELL_KNOWN_TYPES_SYNTAX_2 =
            new WellKnownTypesTestSyntax2.WellKnownTypesSyntax3
            {
                A = 101,
                Floating = 0,
                F1 = new Timestamp { Seconds = 123, Nanos = 1 },
                F2 = "stringValue",
                F4 = new Empty(),
                F5 = new Duration { Nanos = 5, Seconds = 10 },
                F22 = new ListValue { Values = { new Value { NumberValue = 2.2 } } },
                F27 = 27,
                F33 = new Struct(),
                F35 = 64L,
                F37 = new Api { Name = "newapi" },
                F42 = new Enum { Enumvalue = { new EnumValue { Name = "enumValue" } } },
                F47 = new Method { Name = "method", RequestTypeUrl = "sampleUrl" },
                F48 = new Mixin { Name = "mixin" },
                F9 = CalendarPeriod.Day,
                F10 = new Color { Red = 100, Green = 100, Blue = 1000 },
                F7 = new Date { Day = 1, Month = 4, Year = 2022 },
                F13 = new Fraction { Denominator = 100, Numerator = 9 },
                F6 = new Money { Units = 10 },
                F14 = Month.April,
                F16 = new PostalAddress { PostalCode = "98121" },
                F15 = new PhoneNumber { E164Number = "206" },
            };

        public static readonly WellKnownTypesTestSyntax3.WellKnownTypesSyntax3 WELL_KNOWN_TYPES_SYNTAX_3 =
            new WellKnownTypesTestSyntax3.WellKnownTypesSyntax3
            {
                A = 101,
                Floating = 0,
                F1 = new Timestamp { Seconds = 123, Nanos = 1 },
                F2 = "stringValue",
                F4 = new Empty(),
                F5 = new Duration { Nanos = 5, Seconds = 10 },
                F22 = new ListValue { Values = { new Value { NumberValue = 2.2 } } },
                F27 = 27,
                F33 = new Struct(),
                F35 = 64L,
                F37 = new Api { Name = "newapi" },
                F42 = new Enum { Enumvalue = { new EnumValue { Name = "enumValue" } } },
                F47 = new Method { Name = "method", RequestTypeUrl = "sampleUrl" },
                F48 = new Mixin { Name = "mixin" },
                F9 = CalendarPeriod.Day,
                F10 = new Color { Red = 100, Green = 100, Blue = 1000 },
                F7 = new Date { Day = 1, Month = 4, Year = 2022 },
                F13 = new Fraction { Denominator = 100, Numerator = 9 },
                F6 = new Money { Units = 10 },
                F14 = Month.April,
                F16 = new PostalAddress { PostalCode = "98121" },
                F15 = new PhoneNumber { E164Number = "206" },
            };
    }
}
