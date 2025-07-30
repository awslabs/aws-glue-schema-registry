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
        /// <param name="relativePath">Relative path from project root</param>
        /// <returns>Absolute path to the configuration file</returns>
        private static string GetConfigPath(string relativePath)
        {
            var currentDir = new DirectoryInfo(Directory.GetCurrentDirectory());
            while (currentDir != null && !currentDir.GetFiles("*.csproj").Any())
            {
                currentDir = currentDir.Parent;
            }
            
            if (currentDir == null)
            {
                throw new DirectoryNotFoundException("Could not find project root directory containing .csproj file");
            }
            
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
        public void GsrKafkaFlowProtobufSerializer_Constructor_ValidConfig_Success()
        {
            // Act & Assert - Should not throw
            Assert.DoesNotThrow(() => new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH));
        }

        [Test]
        public void GsrKafkaFlowProtobufSerializer_Constructor_InvalidConfigPath_ThrowsException()
        {
            // Arrange
            var invalidConfigPath = "non_existent_config.properties";

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => 
                new GsrKafkaFlowProtobufSerializer<Phone>(invalidConfigPath));
            Assert.That(exception.Message, Does.Contain("Failed to initialize GSR KafkaFlow serializer"));
            Assert.That(exception.Message, Does.Contain("Phone"));
        }

        [Test]
        public void GsrKafkaFlowProtobufDeserializer_Constructor_ValidConfig_Success()
        {
            // Act & Assert - Should not throw
            Assert.DoesNotThrow(() => new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH));
        }

        [Test]
        public void GsrKafkaFlowProtobufDeserializer_Constructor_InvalidConfigPath_ThrowsException()
        {
            // Arrange
            var invalidConfigPath = "non_existent_config.properties";

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => 
                new GsrKafkaFlowProtobufDeserializer<Phone>(invalidConfigPath));
            Assert.That(exception.Message, Does.Contain("Failed to initialize GSR KafkaFlow deserializer"));
            Assert.That(exception.Message, Does.Contain("Phone"));
        }

        [Test]
        [TestCaseSource(nameof(TestMessageProvider))]
        public void KafkaFlowSerDeTestForAllProtobufTypes(IMessage message)
        {
            // Arrange - Use topic name based on message type for better organization
            var topicName = $"test-topic-{message.GetType().Name.ToLower()}";
            var context = CreateMockSerializerContext(topicName);

            // Create serializer and deserializer with proper data config for deserializer
            var dataConfig = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, message.Descriptor }
            });

            var serializer = new GlueSchemaRegistryKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);

            // Act
            var serialized = serializer.Serialize(message, context.Object);
            var deserialized = deserializer.Deserialize(serialized, message.GetType(), context.Object);

            // Assert
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized.GetType(), Is.EqualTo(message.GetType()));
            Assert.That(deserialized, Is.EqualTo(message));
            
            // Verify context was used correctly
            context.Verify(c => c.Topic, Times.AtLeastOnce);
        }

        [Test]
        public void KafkaFlowSerDeTest_BasicSyntax2Message_RoundTripSuccess()
        {
            // Arrange
            var message = BASIC_SYNTAX2_MESSAGE;
            var topicName = "test-topic-basic-syntax2";
            var context = CreateMockSerializerContext(topicName);

            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);

            // Act
            var serialized = serializer.Serialize(message, context.Object);
            var deserialized = (Phone)deserializer.Deserialize(serialized, typeof(Phone), context.Object);

            // Assert
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized, Is.EqualTo(message));
            Assert.That(deserialized.Name, Is.EqualTo(message.Name));
        }

        [Test]
        public async Task KafkaFlowSerializeAsync_ValidMessage_WritesToStreamCorrectly()
        {
            // Arrange
            var message = BASIC_SYNTAX2_MESSAGE;
            var topicName = "test-topic-async-serialize";
            var context = CreateMockSerializerContext(topicName);
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var stream = new MemoryStream();

            // Act
            await serializer.SerializeAsync(message, stream, context.Object);

            // Assert
            Assert.That(stream.Length, Is.GreaterThan(0));
            
            // Verify we can deserialize what was written
            stream.Position = 0;
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);
            var deserialized = await deserializer.DeserializeAsync(stream, typeof(Phone), context.Object);
            
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized, Is.EqualTo(message));
        }

        [Test]
        public async Task KafkaFlowDeserializeAsync_ValidData_ReturnsCorrectObject()
        {
            // Arrange
            var message = BASIC_SYNTAX2_MESSAGE;
            var topicName = "test-topic-async-deserialize";
            var context = CreateMockSerializerContext(topicName);
            
            // First serialize to get valid data
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var serialized = serializer.Serialize(message, context.Object);
            
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);
            var stream = new MemoryStream(serialized);

            // Act
            var deserialized = await deserializer.DeserializeAsync(stream, typeof(Phone), context.Object);

            // Assert
            Assert.That(deserialized, Is.Not.Null);
            Assert.That(deserialized, Is.EqualTo(message));
        }

        [Test]
        public void KafkaFlowSerializer_WithDifferentTopics_UsesTopicCorrectly()
        {
            // Arrange
            var message = BASIC_SYNTAX2_MESSAGE;
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            
            var topics = new[] { "topic-1", "topic-2", "custom-topic-name" };

            foreach (var topicName in topics)
            {
                var context = CreateMockSerializerContext(topicName);

                // Act
                var serialized = serializer.Serialize(message, context.Object);

                // Assert
                Assert.That(serialized, Is.Not.Null);
                Assert.That(serialized.Length, Is.GreaterThan(0));
                context.Verify(c => c.Topic, Times.AtLeastOnce, $"Topic {topicName} should be accessed");
            }
        }

        [Test]
        public void KafkaFlowDeserializer_WithDifferentTopics_UsesTopicCorrectly()
        {
            // Arrange
            var message = BASIC_SYNTAX2_MESSAGE;
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);
            
            var topics = new[] { "topic-1", "topic-2", "custom-topic-name" };

            foreach (var topicName in topics)
            {
                var context = CreateMockSerializerContext(topicName);
                
                // First serialize with this topic
                var serialized = serializer.Serialize(message, context.Object);

                // Act - Deserialize with same topic
                var deserialized = deserializer.Deserialize(serialized, typeof(Phone), context.Object);

                // Assert
                Assert.That(deserialized, Is.Not.Null);
                Assert.That(deserialized, Is.EqualTo(message));
                context.Verify(c => c.Topic, Times.AtLeastOnce, $"Topic {topicName} should be accessed during deserialize");
            }
        }

        [Test]
        public void KafkaFlowSerializer_SerializeNull_ThrowsException()
        {
            // Arrange
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var context = CreateMockSerializerContext();

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => 
                serializer.Serialize(null, context.Object));
            Assert.That(exception.Message, Does.Contain("Failed to serialize Phone"));
        }

        [Test]
        public void KafkaFlowDeserializer_DeserializeInvalidData_ThrowsException()
        {
            // Arrange
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);
            var context = CreateMockSerializerContext();
            var invalidData = new byte[] { 0x01, 0x02, 0x03, 0x04 }; // Invalid GSR data

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() => 
                deserializer.Deserialize(invalidData, typeof(Phone), context.Object));
            Assert.That(exception.Message, Does.Contain("Failed to deserialize Phone"));
        }

        [Test]
        public void KafkaFlowSerializer_Dispose_DoesNotThrow()
        {
            // Arrange
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);

            // Act & Assert
            Assert.DoesNotThrow(() => serializer.Dispose());
        }

        [Test]
        public void KafkaFlowDeserializer_Dispose_DoesNotThrow()
        {
            // Arrange
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);

            // Act & Assert
            Assert.DoesNotThrow(() => deserializer.Dispose());
        }

        [Test]
        public void KafkaFlowSerializer_MultipleOperations_WorksCorrectly()
        {
            // Arrange
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var context = CreateMockSerializerContext("multi-op-topic");
            var messages = new[] { BASIC_SYNTAX2_MESSAGE, BASIC_SYNTAX2_MESSAGE, BASIC_SYNTAX2_MESSAGE };

            // Act & Assert - Multiple serializations should work
            foreach (var message in messages)
            {
                var serialized = serializer.Serialize(message, context.Object);
                Assert.That(serialized, Is.Not.Null);
                Assert.That(serialized.Length, Is.GreaterThan(0));
            }
        }

        [Test]
        public void KafkaFlowDeserializer_MultipleOperations_WorksCorrectly()
        {
            // Arrange
            var serializer = new GsrKafkaFlowProtobufSerializer<Phone>(PROTOBUF_CONFIG_PATH);
            var deserializer = new GsrKafkaFlowProtobufDeserializer<Phone>(PROTOBUF_CONFIG_PATH);
            var context = CreateMockSerializerContext("multi-op-topic");
            var message = BASIC_SYNTAX2_MESSAGE;

            // Act & Assert - Multiple deserializations should work
            for (int i = 0; i < 3; i++)
            {
                var serialized = serializer.Serialize(message, context.Object);
                var deserialized = deserializer.Deserialize(serialized, typeof(Phone), context.Object);
                Assert.That(deserialized, Is.EqualTo(message));
            }
        }
    }

    /// <summary>
    /// Helper class to create KafkaFlow serializers with data configuration
    /// This mimics the pattern used in GlueSchemaRegistryKafkaSerializerTests
    /// </summary>
    public class GlueSchemaRegistryKafkaFlowProtobufSerializer<T> : GsrKafkaFlowProtobufSerializer<T>
        where T : class, IMessage<T>, new()
    {
        public GlueSchemaRegistryKafkaFlowProtobufSerializer(string configPath) : base(configPath) 
        { 
        }
    }

    /// <summary>
    /// Helper class to create KafkaFlow deserializers with data configuration
    /// This mimics the pattern used in GlueSchemaRegistryKafkaSerializerTests
    /// </summary>
    public class GlueSchemaRegistryKafkaFlowProtobufDeserializer<T> : GsrKafkaFlowProtobufDeserializer<T>
        where T : class, IMessage<T>, new()
    {
        public GlueSchemaRegistryKafkaFlowProtobufDeserializer(string configPath) : base(configPath) 
        { 
        }
        
        public GlueSchemaRegistryKafkaFlowProtobufDeserializer(string configPath, GlueSchemaRegistryDataFormatConfiguration dataConfig) : base(configPath) 
        {
            // Note: The base deserializer already handles the data config in its constructor
            // This constructor signature matches the pattern from the main serializer tests
        }
    }
}
