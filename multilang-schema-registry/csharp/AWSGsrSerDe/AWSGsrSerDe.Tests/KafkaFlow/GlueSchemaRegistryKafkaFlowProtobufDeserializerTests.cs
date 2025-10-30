using System;
using System.IO;
using System.Threading.Tasks;
using AWSGsrSerDe.KafkaFlow;
using Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2;
using KafkaFlow;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.KafkaFlow
{
    [TestFixture]
    public class GlueSchemaRegistryKafkaFlowProtobufDeserializerTests
    {
        private const string ValidConfigPath = "deserializer-test-config.properties";

        [SetUp]
        public void Setup()
        {
            // Create a minimal config file for testing
            var configContent = @"region=us-east-1
                registry.name=default-registry
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
        public void Deserialize_WithValidSerializedData_ReturnsOriginalData()
        {
            // Arrange
            var originalCustomer = new Customer { Name = "John Doe" };
            var context = new TestDeserializerContext("deserializer-test-topic");

            var serializer = new GlueSchemaRegistryKafkaFlowProtobufSerializer<Customer>(ValidConfigPath);
            var deserializer = new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Customer>(ValidConfigPath);

            // First serialize the data
            var serializedData = serializer.Serialize(originalCustomer, context);

            // Act - Deserialize
            var deserializedCustomer = (Customer)deserializer.Deserialize(serializedData, typeof(Customer), context);

            // Assert
            Assert.IsNotNull(deserializedCustomer);
            Assert.AreEqual(originalCustomer.Name, deserializedCustomer.Name);
            Assert.AreEqual(originalCustomer.HasName, deserializedCustomer.HasName);
            Assert.AreEqual(originalCustomer, deserializedCustomer);
        }

        [Test]
        public async Task DeserializeAsync_WithValidSerializedData_ReturnsOriginalData()
        {
            // Arrange
            var originalAddress = new Address
            {
                Street = "123 Main St",
                City = "Test City",
                Zip = 12345
            };
            var context = new TestDeserializerContext("deserializer-async-test-topic");

            var serializer = new GlueSchemaRegistryKafkaFlowProtobufSerializer<Address>(ValidConfigPath);
            var deserializer = new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Address>(ValidConfigPath);
            using var stream = new MemoryStream();

            // First serialize the data to stream
            await serializer.SerializeAsync(originalAddress, stream, context);

            // Reset stream position for reading
            stream.Position = 0;

            // Act - Deserialize from stream
            var deserializedAddress = (Address)await deserializer.DeserializeAsync(stream, typeof(Address), context);

            // Assert
            Assert.IsNotNull(deserializedAddress);
            Assert.AreEqual(originalAddress.Street, deserializedAddress.Street);
            Assert.AreEqual(originalAddress.City, deserializedAddress.City);
            Assert.AreEqual(originalAddress.Zip, deserializedAddress.Zip);
            Assert.AreEqual(originalAddress, deserializedAddress);
        }

        [Test]
        public void Constructor_WithInvalidConfigPath_ThrowsException()
        {
            // Arrange
            var invalidConfigPath = "non-existent-deserializer-config.properties";

            // Act & Assert
            var exception = Assert.Throws<InvalidOperationException>(() =>
                new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Customer>(invalidConfigPath));

            Assert.That(exception.Message, Does.Contain("Failed to initialize GlueSchemaRegistry KafkaFlow deserializer"));
        }

        [Test]
        public void Deserialize_WithNullContext_ThrowsException()
        {
            // Arrange
            var deserializer = new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Customer>(ValidConfigPath);
            var testData = new byte[] { 0x01, 0x02, 0x03 };

            // Act & Assert
            var exception = Assert.Throws<ArgumentNullException>(() =>
                deserializer.Deserialize(testData, typeof(Customer), null));

            Assert.That(exception.ParamName, Is.EqualTo("context"));
        }

        
        // TODO: Add DeserializeAsync failure tests
    }

    /// <summary>
    /// Test implementation of ISerializerContext for testing purposes
    /// </summary>
    public class TestDeserializerContext : ISerializerContext
    {
        public TestDeserializerContext(string topic)
        {
            Topic = topic;
        }

        public string Topic { get; }
    }
}
