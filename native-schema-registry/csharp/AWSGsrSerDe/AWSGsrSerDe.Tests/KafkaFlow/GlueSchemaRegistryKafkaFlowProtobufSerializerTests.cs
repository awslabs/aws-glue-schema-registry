using System;
using System.IO;
using System.Threading.Tasks;
using AWSGsrSerDe.KafkaFlow;
using Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2;
using KafkaFlow;
using NUnit.Framework;

namespace AWSGlueSchemaRegistrySerDe.Tests.KafkaFlow
{
    [TestFixture]
    public class GlueSchemaRegistryKafkaFlowProtobufSerializerTests
    {
        private const string ValidConfigPath = "test-config.properties";

        [SetUp]
        public void Setup()
        {
            // Create a minimal config file for testing
            var configContent = @"region=us-east-1
                registry.name=test-registry
                dataFormat=PROTOBUF
                schemaAutoRegistrationEnabled=true";
            if (File.Exists(ValidConfigPath))
            {
                File.Delete(ValidConfigPath);
            }
            File.WriteAllText(ValidConfigPath, configContent);
        }

        [TearDown]
        public void TearDown()
        {
            if (File.Exists(ValidConfigPath))
            {
                File.Delete(ValidConfigPath);
            }
        }

        [Test]
        public void SerializeDeserialize_Roundtrip_PreservesData()
        {
            // Arrange
            var originalCustomer = new Customer { Name = "John Doe" };
            var context = new TestSerializerContext("test-topic-kafka-flow");

            var serializer = new GlueSchemaRegistryKafkaFlowProtobufSerializer<Customer>(ValidConfigPath);
            var deserializer = new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Customer>(ValidConfigPath);

            // Act - Serialize
            var serializedData = serializer.Serialize(originalCustomer, context);

            // Act - Deserialize
            var deserializedCustomer = (Customer)deserializer.Deserialize(serializedData, typeof(Customer), context);

            // Assert
            Assert.IsNotNull(deserializedCustomer);
            Assert.AreEqual(originalCustomer.Name, deserializedCustomer.Name);
            Assert.AreEqual(originalCustomer.HasName, deserializedCustomer.HasName);

        }

        [Test]
        public async Task SerializeDeserializeAsync_Roundtrip_PreservesData()
        {
            // Arrange
            var originalCustomer = new Customer { Name = "Jane Smith" };
            var context = new TestSerializerContext("async-test-topic-kafka-flow");

            var serializer = new GlueSchemaRegistryKafkaFlowProtobufSerializer<Customer>(ValidConfigPath);
            var deserializer = new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Customer>(ValidConfigPath);
            using var stream = new MemoryStream();

            // Act - Serialize to stream
            await serializer.SerializeAsync(originalCustomer, stream, context);

            // Reset stream position for reading
            stream.Position = 0;

            // Act - Deserialize from stream
            var deserializedCustomer = (Customer)await deserializer.DeserializeAsync(stream, typeof(Customer), context);

            // Assert
            Assert.IsNotNull(deserializedCustomer);
            Assert.AreEqual(originalCustomer.Name, deserializedCustomer.Name);
            Assert.AreEqual(originalCustomer.HasName, deserializedCustomer.HasName);
        }

        [Test]
        public void Constructor_WithInvalidConfigPath_ThrowsException()
        {
            // Arrange
            var invalidConfigPath = "non-existent-config.properties";

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() =>
                new GlueSchemaRegistryKafkaFlowProtobufSerializer<Customer>(invalidConfigPath));

            Assert.That(exception.Message, Does.Contain("Failed to initialize GlueSchemaRegistry KafkaFlow serializer"));
        }

        [Test]
        public void Serialize_WithNullMessage_ThrowsException()
        {
            // Arrange
            var serializer = new GlueSchemaRegistryKafkaFlowProtobufSerializer<Customer>(ValidConfigPath);
            var context = new TestSerializerContext("test-topic-kf-seriailizer");

            // Act & Assert
            var exception = Assert.Throws<ArgumentNullException>(() =>
                serializer.Serialize(null, context));

            Assert.That(exception.ParamName, Is.EqualTo("message"));
        }

        [Test]
        public void Serialize_WithNullContext_ThrowsException()
        {
            // Arrange
            var customer = new Customer { Name = "Test Customer" };
            var serializer = new GlueSchemaRegistryKafkaFlowProtobufSerializer<Customer>(ValidConfigPath);

            // Act & Assert
            var exception = Assert.Throws<ArgumentNullException>(() =>
                serializer.Serialize(customer, null));

            Assert.That(exception.ParamName, Is.EqualTo("context"));
        }
        
        // TODO: Add async tests for SerializeAsync methods
    }

    /// <summary>
    /// Test implementation of ISerializerContext for testing purposes
    /// </summary>
    public class TestSerializerContext : ISerializerContext
    {
        public TestSerializerContext(string topic)
        {
            Topic = topic;
        }

        public string Topic { get; }
    }
}
