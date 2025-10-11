using System;
using System.Threading.Tasks;
using Amazon.Glue.Model;
using NUnit.Framework;
using AWSGsrSerDe.Tests.utils;
using Avro;
using Avro.Generic;

namespace AWSGsrSerDe.Tests.EvolutionTests
{
    [TestFixture]
    public class AvroEvolutionTests : IntegrationTestBase
    {
        private string _schemaName;

        [SetUp]
        public async Task SetUp()
        {
            _schemaName = $"avro-evolution-test-{Guid.NewGuid():N}";
            
            // Ensure the registry exists for our tests
            await EnsureRegistryExists(CUSTOM_REGISTRY_NAME, DEFAULT_REGION);
        }

        [Test]
        public async Task AvroBackwardEvolution_RegistersVersionsSuccessfully()
        {
            var serializer = CreateSerializer("minimal-auto-registration-custom-registry.properties");

            // V1: Create record from shared user_v1.avsc but modify schema name to be consistent
            var v1SchemaText = LoadSharedAvroSchema("backward/user_v1.avsc");
            v1SchemaText = v1SchemaText.Replace("\"name\": \"user_v1\"", "\"name\": \"User\"");
            var v1Schema = Schema.Parse(v1SchemaText);
            var v1Record = new GenericRecord((RecordSchema)v1Schema);
            v1Record.Add("id", "user123");
            v1Record.Add("name", "John Doe");
            v1Record.Add("email", "john@example.com");
            v1Record.Add("active", true);
            
            var v1Bytes = serializer.Serialize(v1Record, _schemaName);
            Assert.NotNull(v1Bytes, "V1 serialized bytes should not be null");

            // V2: Create record from shared user_v2.avsc but modify schema name to be consistent
            var v2SchemaText = LoadSharedAvroSchema("backward/user_v2.avsc");
            v2SchemaText = v2SchemaText.Replace("\"name\": \"user_v2\"", "\"name\": \"User\"");
            var v2Schema = Schema.Parse(v2SchemaText);
            var v2Record = new GenericRecord((RecordSchema)v2Schema);
            v2Record.Add("id", "user456");
            v2Record.Add("name", "Jane Doe");
            v2Record.Add("active", true);
            v2Record.Add("status", "verified");
            
            var v2Bytes = serializer.Serialize(v2Record, _schemaName);
            Assert.NotNull(v2Bytes, "V2 serialized bytes should not be null");

            // V3: Create record from shared user_v3.avsc but modify schema name to be consistent
            var v3SchemaText = LoadSharedAvroSchema("backward/user_v3.avsc");
            v3SchemaText = v3SchemaText.Replace("\"name\": \"user_v3\"", "\"name\": \"User\"");
            var v3Schema = Schema.Parse(v3SchemaText);
            var v3Record = new GenericRecord((RecordSchema)v3Schema);
            v3Record.Add("id", "user789");
            v3Record.Add("name", "Bob Smith");
            v3Record.Add("active", true);
            v3Record.Add("status", "premium");
            v3Record.Add("age", 35);
            
            var v3Bytes = serializer.Serialize(v3Record, _schemaName);
            Assert.NotNull(v3Bytes, "V3 serialized bytes should not be null");

            // Mark schema for cleanup
            MarkSchemaForCleanup(CUSTOM_REGISTRY_NAME, _schemaName, DEFAULT_REGION);

            // Wait for schema registration to complete
            var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, _schemaName, DEFAULT_REGION);
            Assert.IsTrue(schemaRegistered, "Schema should have been registered");

            // Verify 3 versions exist
            var glueClient = GetGlueClientForRegion(DEFAULT_REGION);
            var versionsResponse = await glueClient.ListSchemaVersionsAsync(new ListSchemaVersionsRequest
            {
                SchemaId = new SchemaId
                {
                    RegistryName = CUSTOM_REGISTRY_NAME,
                    SchemaName = _schemaName
                }
            });

            Assert.That(versionsResponse.Schemas.Count, Is.EqualTo(3), "Should have 3 schema versions");
            Assert.That(versionsResponse.Schemas[2].VersionNumber, Is.EqualTo(1));
            Assert.That(versionsResponse.Schemas[1].VersionNumber, Is.EqualTo(2));
            Assert.That(versionsResponse.Schemas[0].VersionNumber, Is.EqualTo(3));
            
            Console.WriteLine($"✓ Successfully registered 3 backward compatible schema versions");
        }

        [Test]
        public async Task AvroBackwardEvolution_IncompatibleChange_ThrowsException()
        {
            var serializer = CreateSerializer("minimal-auto-registration-custom-registry.properties");

            // V1: Create record from shared employee_v1.avsc
            var v1SchemaText = LoadSharedAvroSchema("negative/backward/employee_v1.avsc");
            var v1Schema = Schema.Parse(v1SchemaText);
            var v1Record = new GenericRecord((RecordSchema)v1Schema);
            v1Record.Add("id", 123);
            v1Record.Add("firstName", "John");
            v1Record.Add("lastName", "Doe");
            v1Record.Add("department", "Engineering");
            
            var v1Bytes = serializer.Serialize(v1Record, _schemaName);
            Assert.NotNull(v1Bytes, "V1 serialized bytes should not be null");

            // Mark schema for cleanup
            MarkSchemaForCleanup(CUSTOM_REGISTRY_NAME, _schemaName, DEFAULT_REGION);

            // Wait for V1 schema registration to complete
            var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, _schemaName, DEFAULT_REGION);
            Assert.IsTrue(schemaRegistered, "V1 schema should have been registered");

            // Incompatible: Use employee_v2.avsc which adds required field (breaks backward compatibility)
            var incompatibleSchemaText = LoadSharedAvroSchema("negative/backward/employee_v2.avsc");
            var incompatibleSchema = Schema.Parse(incompatibleSchemaText);
            var incompatibleRecord = new GenericRecord((RecordSchema)incompatibleSchema);
            incompatibleRecord.Add("id", 456);
            incompatibleRecord.Add("firstName", "Jane");
            incompatibleRecord.Add("lastName", "Smith");
            incompatibleRecord.Add("department", "Marketing");
            incompatibleRecord.Add("requiredEmail", "jane@example.com");
            
            // Should throw exception due to backward incompatible change (added required field)
            Assert.ThrowsAsync<AwsSchemaRegistryException>(async () =>
            {
                serializer.Serialize(incompatibleRecord, _schemaName);
            });
            
            Console.WriteLine($"✓ Successfully validated backward incompatible change throws exception");
        }
    }
}
