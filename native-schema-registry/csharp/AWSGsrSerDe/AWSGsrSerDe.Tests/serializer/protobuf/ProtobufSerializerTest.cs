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

using System;
using System.Collections.Generic;
using System.Text;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer.protobuf;
using AWSGsrSerDe.serializer.protobuf;
using Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2.Basic;
using Google.Protobuf;
using NUnit.Framework;
using static AWSGsrSerDe.common.GlueSchemaRegistryConstants.DataFormat;
using static AWSGsrSerDe.Tests.utils.ProtobufGenerator;

namespace AWSGsrSerDe.Tests.serializer.protobuf
{
    [TestFixture]
    public class ProtobufSerializerTest
    {
        [Test]
        public void TestValidate_InvalidObject_ThrowsException()
        {
            var protobufSerializer = new ProtobufSerializer();
            var str = "test";
            var ex = Assert.Throws(typeof(AwsSchemaRegistryException), () => protobufSerializer.Validate(str));
            Assert.AreEqual("Object is not of Message type: System.String", ex.Message);

            var num = 5;
            ex = Assert.Throws(typeof(AwsSchemaRegistryException), () => protobufSerializer.Validate(num));
            Assert.AreEqual("Object is not of Message type: System.Int32", ex.Message);

            ex = Assert.Throws(typeof(ArgumentNullException), () => protobufSerializer.Validate(null));
            Assert.AreEqual("Value cannot be null. (Parameter 'data')", ex.Message);
        }

        private static List<IMessage> TestProtobufMessageProvider()
        {
            return new List<IMessage>
            {
                NESTING_MESSAGE_PROTO2,
                NESTING_MESSAGE_PROTO3,
                BASIC_SYNTAX3_MESSAGE,
                BASIC_SYNTAX2_MESSAGE, DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE,
                UNICODE_MESSAGE,
                DOLLAR_SYNTAX_3_MESSAGE,
            };
        }

        [Test]
        [TestCaseSource(nameof(TestProtobufMessageProvider))]
        public void TestGetSchemaDefinition_GeneratesValidSchemaDefinition_ForAllTypesOfMessages(IMessage message)
        {
            var protobufSerializer = new ProtobufSerializer();
            var schemaDefinition = protobufSerializer.GetSchemaDefinition(message);

            // Ensure the output schema Definition is a Base64 encoded string
            var buffer = new Span<byte>(new byte[schemaDefinition.Length]);
            Assert.IsTrue(Convert.TryFromBase64String(schemaDefinition, buffer, out _));

            Assert.AreEqual(message.Descriptor.File.SerializedData.ToBase64(), schemaDefinition);

            // Ensure the package name is part of schemaDefinition
            var decodedSchemaDefinition = Encoding.Default.GetString(buffer.ToArray());
            Assert.IsTrue(decodedSchemaDefinition.Contains(message.Descriptor.File.Package));
        }

        private static List<IMessage> TestMessageProvider()
        {
            return new List<IMessage>
            {
                BASIC_SYNTAX2_MESSAGE,
                BASIC_SYNTAX3_MESSAGE,
                BASIC_REFERENCING_MESSAGE,
                DOTNET_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE,
                NESTING_MESSAGE_PROTO2,
                NESTING_MESSAGE_PROTO3,
                SNAKE_CASE_MESSAGE,
                ANOTHER_SNAKE_CASE_MESSAGE,
                DOLLAR_SYNTAX_3_MESSAGE,
                HYPHEN_ATED_PROTO_FILE_MESSAGE,
                DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE,
                UNICODE_MESSAGE,
                CONFLICTING_NAME_MESSAGE,
                NESTED_CONFLICTING_NAME_MESSAGE,
                NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
                ALL_TYPES_MESSAGE_SYNTAX2,
                ALL_TYPES_MESSAGE_SYNTAX3,
                WELL_KNOWN_TYPES_SYNTAX_2,
                WELL_KNOWN_TYPES_SYNTAX_3,
            };
        }

        [Test]
        [TestCaseSource(nameof(TestMessageProvider))]
        public void TestSerialize_ProducesValidDeserializableBytes_ForAllTypesOfMessages(IMessage message)
        {
            var config = new GlueSchemaRegistryConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, message.Descriptor},
            });
            var protobufSerializer = new ProtobufSerializer();
            var serializedBytes = protobufSerializer.Serialize(message);

            var protobufDeserializer = new ProtobufDeserializer(config);
            var deserializeMessage = protobufDeserializer.Deserialize(
                serializedBytes,
                new GlueSchemaRegistrySchema(message.Descriptor.Name, "dummy schema def", PROTOBUF.ToString()));
            Assert.AreEqual(message, deserializeMessage);
        }
    }
}
