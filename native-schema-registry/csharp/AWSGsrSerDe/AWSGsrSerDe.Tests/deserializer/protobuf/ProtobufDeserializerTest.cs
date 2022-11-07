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

namespace AWSGsrSerDe.Tests.deserializer.protobuf
{
    [TestFixture]
    public class ProtobufDeserializerTest
    {
        [Test]
        public void TestConstruct_NullArg_ThrowsException()
        {
            var ex = Assert.Throws(typeof(ArgumentNullException), () => new ProtobufDeserializer(null));
            Assert.AreEqual("Value cannot be null. (Parameter 'config')", ex.Message);
        }

        [Test]
        public void TestConstruct_ValidArg_Succeed()
        {
            var config = new GlueSchemaRegistryConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, Phone.Descriptor },
            });
            var protobufDeserializer = new ProtobufDeserializer(config);
            Assert.NotNull(protobufDeserializer);
        }

        [Test]
        public void TestDeserialize_NullArgs_ThrowsException()
        {
            var config = new GlueSchemaRegistryConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, Phone.Descriptor },
            });
            var protobufSerializer = new ProtobufSerializer();
            var protobufDeserializer = new ProtobufDeserializer(config);
            var schema = new GlueSchemaRegistrySchema(
                "Basic",
                protobufSerializer.GetSchemaDefinition(BASIC_SYNTAX2_MESSAGE),
                PROTOBUF.ToString());
            var ex = Assert.Throws(typeof(ArgumentNullException), () => protobufDeserializer.Deserialize(null, schema));
            Assert.AreEqual("Value cannot be null. (Parameter 'data')", ex.Message);
            ex = Assert.Throws(
                typeof(ArgumentNullException),
                () => protobufDeserializer.Deserialize(protobufSerializer.Serialize(BASIC_SYNTAX2_MESSAGE), null));
            Assert.AreEqual("Value cannot be null. (Parameter 'schema')", ex.Message);
        }

        private static List<IMessage> TestDeserializationMessageProvider()
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
            };
        }

        [Test]
        [TestCaseSource(nameof(TestDeserializationMessageProvider))]
        public void TestDeserialize_Succeed_ForAllTypesOfMessages(IMessage message)
        {
            var config = new GlueSchemaRegistryConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, message.Descriptor },
            });
            var protobufSerializer = new ProtobufSerializer();
            var serializedBytes = protobufSerializer.Serialize(message);

            var protobufDeserializer = new ProtobufDeserializer(config);
            var deserializeMessage = protobufDeserializer.Deserialize(
                serializedBytes,
                new GlueSchemaRegistrySchema(message.Descriptor.Name, "dummy schema def", PROTOBUF.ToString()));
            Assert.AreEqual(message, deserializeMessage);
        }

        [Test]
        public void TestDeserialize_InvalidBytes_ThrowsException()
        {
            var config = new GlueSchemaRegistryConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, Phone.Descriptor },
            });
            const string invalid = "invalid";
            var protobufDeserializer = new ProtobufDeserializer(config);
            var ex = Assert.Throws(typeof(AwsSchemaRegistryException), () => protobufDeserializer.Deserialize(
                Encoding.Default.GetBytes(invalid),
                new GlueSchemaRegistrySchema("dummy", "dummy schema def", PROTOBUF.ToString())));
            Assert.AreEqual("Exception occurred while de-serializing Protobuf message", ex.Message);
        }
    }
}
