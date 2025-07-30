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
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AWSGsrSerDe.common;
using AWSGsrSerDe.KafkaFlow;
using Google.Protobuf;
using KafkaFlow;
using Moq;
using NUnit.Framework;
using Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2.Basic;
using static AWSGsrSerDe.Tests.utils.ProtobufGenerator;

namespace AWSGsrSerDe.Tests.KafkaFlow
{
    [TestFixture]
    public class GsrKafkaFlowProtobufTests
    {
        private static readonly string PROTOBUF_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal-protobuf.properties");

        /// <summary>
        /// Finds the project root by looking for .csproj file and returns absolute path to config file
        /// </summary>
        private static string GetConfigPath(string relativePath)
        {
            var currentDir = new DirectoryInfo(Directory.GetCurrentDirectory());
            while (currentDir != null && !currentDir.GetFiles("*.csproj").Any())
            {
                currentDir = currentDir.Parent;
            }
            if (currentDir == null)
                throw new DirectoryNotFoundException("Could not find project root directory containing .csproj file");
            return Path.Combine(currentDir.FullName, relativePath);
        }

        private static List<IMessage> TestMessageProvider()
        {
            return new List<IMessage>
            {
                BASIC_SYNTAX2_MESSAGE,
                BASIC_SYNTAX3_MESSAGE,
                BASIC_REFERENCING_MESSAGE,
                NESTING_MESSAGE_PROTO2,
                NESTING_MESSAGE_PROTO3,
                NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
                ALL_TYPES_MESSAGE_SYNTAX2,
                ALL_TYPES_MESSAGE_SYNTAX3,
                WELL_KNOWN_TYPES_SYNTAX_2,
                WELL_KNOWN_TYPES_SYNTAX_3,
            };
        }

        private Mock<ISerializerContext> CreateMockSerializerContext(string topic = "test-topic")
        {
            var context = new Mock<ISerializerContext>();
            context.Setup(c => c.Topic).Returns(topic);
            return context;
        }

        [Test]
        [TestCaseSource(nameof(TestMessageProvider))]
        public void KafkaFlowSerDe_Sync_RoundTrip_AllTypes(IMessage message)
        {
            var topicName = $"test-topic-{message.GetType().Name.ToLower()}";
            var context = CreateMockSerializerContext(topicName);

            var messageType = message.GetType();
            var configPath = PROTOBUF_CONFIG_PATH;

            // Dynamically create serializer and deserializer for this message type
            var serializerType = typeof(GsrKafkaFlowProtobufSerializer<>).MakeGenericType(messageType);
            dynamic serializer = Activator.CreateInstance(serializerType, configPath);

            var deserializerType = typeof(GsrKafkaFlowProtobufDeserializer<>).MakeGenericType(messageType);
            dynamic deserializer = Activator.CreateInstance(deserializerType, configPath);

            // Act
            byte[] serialized = serializer.Serialize((dynamic)message, context.Object);
            object deserialized = deserializer.Deserialize(serialized, messageType, context.Object);

            // Assert
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized.GetType(), Is.EqualTo(messageType));
            Assert.That(deserialized, Is.EqualTo(message));
            context.Verify(c => c.Topic, Times.AtLeastOnce);
        }

        [Test]
        [TestCaseSource(nameof(TestMessageProvider))]
        public async Task KafkaFlowSerDe_Async_RoundTrip_AllTypes(IMessage message)
        {
            var topicName = $"test-topic-async-{message.GetType().Name.ToLower()}";
            var context = CreateMockSerializerContext(topicName);

            var messageType = message.GetType();
            var configPath = PROTOBUF_CONFIG_PATH;

            // Dynamically create serializer and deserializer for this message type
            var serializerType = typeof(GsrKafkaFlowProtobufSerializer<>).MakeGenericType(messageType);
            dynamic serializer = Activator.CreateInstance(serializerType, configPath);

            var deserializerType = typeof(GsrKafkaFlowProtobufDeserializer<>).MakeGenericType(messageType);
            dynamic deserializer = Activator.CreateInstance(deserializerType, configPath);

            // Serialize to stream
            using var stream = new MemoryStream();
            await serializer.SerializeAsync((dynamic)message, stream, context.Object);

            // Reset stream and deserialize
            stream.Position = 0;
            object deserialized = await deserializer.DeserializeAsync(stream, messageType, context.Object);

            // Assert
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized.GetType(), Is.EqualTo(messageType));
            Assert.That(deserialized, Is.EqualTo(message));
            context.Verify(c => c.Topic, Times.AtLeastOnce);
        }

        [Test]
        public void GsrKafkaFlowProtobufSerializer_Constructor_ValidConfig_Success()
        {
            // Example with Phone, just to show type-specific tests are still allowed
            Assert.DoesNotThrow(() => new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH));
        }

        [Test]
        public void GsrKafkaFlowProtobufDeserializer_Constructor_ValidConfig_Success()
        {
            Assert.DoesNotThrow(() => new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH));
        }

        [Test]
        public void KafkaFlowSerializer_SerializeNull_ThrowsException()
        {
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var context = CreateMockSerializerContext();
            var exception = Assert.Throws<InvalidOperationException>(() =>
                serializer.Serialize(null, context.Object));
            Assert.That(exception.Message, Does.Contain("Failed to serialize Phone"));
        }

        [Test]
        public void KafkaFlowDeserializer_DeserializeInvalidData_ThrowsException()
        {
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);
            var context = CreateMockSerializerContext();
            var invalidData = new byte[] { 0x01, 0x02, 0x03, 0x04 };
            var exception = Assert.Throws<InvalidOperationException>(() =>
                deserializer.Deserialize(invalidData, typeof(Phone), context.Object));
            Assert.That(exception.Message, Does.Contain("Failed to deserialize Phone"));
        }

        [Test]
        public void KafkaFlowSerializer_Dispose_DoesNotThrow()
        {
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            Assert.DoesNotThrow(() => serializer.Dispose());
        }

        [Test]
        public void KafkaFlowDeserializer_Dispose_DoesNotThrow()
        {
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);
            Assert.DoesNotThrow(() => deserializer.Dispose());
        }
    }
}
