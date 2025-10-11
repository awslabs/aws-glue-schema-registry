using System;
using System.IO;
using System.Threading.Tasks;
using Amazon.Glue;
using Amazon.Glue.Model;
using NUnit.Framework;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.Tests.utils;
using Avro;
using Avro.Generic;

namespace AWSGsrSerDe.Tests.EvolutionTests
{
    [TestFixture]
    public class AvroEvolutionTests
    {
        private const string CUSTOM_REGISTRY_NAME = "native-test-registry";
        private IAmazonGlue _glueClient;
        private string _schemaName;

        [SetUp]
        public void SetUp()
        {
            _glueClient = new AmazonGlueClient();
            _schemaName = $"avro-evolution-test-{Guid.NewGuid():N}";
        }

        [TearDown]
        public async Task TearDown()
        {
            try
            {
                await _glueClient.DeleteSchemaAsync(new DeleteSchemaRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = CUSTOM_REGISTRY_NAME,
                        SchemaName = _schemaName
                    }
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Failed to clean up schema {_schemaName}: {ex.Message}");
            }
            
            _glueClient?.Dispose();
        }

        [Test]
        public async Task AvroBackwardEvolution_RegistersVersionsSuccessfully()
        {
            var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
            var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/minimal-auto-registration-custom-registry.properties");
            configPath = Path.GetFullPath(configPath);

            var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

            // V1: Create record from existing user_v1.avsc but modify schema name to be consistent
            var v1SchemaText = File.ReadAllText(Path.Combine(assemblyDir, "../../../../../../shared/test/avro/backward/user_v1.avsc"));
            v1SchemaText = v1SchemaText.Replace("\"name\": \"user_v1\"", "\"name\": \"User\"");
            var v1Schema = Schema.Parse(v1SchemaText);
            var v1Record = new GenericRecord((RecordSchema)v1Schema);
            v1Record.Add("id", "user123");
            v1Record.Add("name", "John Doe");
            v1Record.Add("email", "john@example.com");
            v1Record.Add("active", true);
            serializer.Serialize(v1Record, _schemaName);

            // V2: Create record from existing user_v2.avsc but modify schema name to be consistent
            var v2SchemaText = File.ReadAllText(Path.Combine(assemblyDir, "../../../../../../shared/test/avro/backward/user_v2.avsc"));
            v2SchemaText = v2SchemaText.Replace("\"name\": \"user_v2\"", "\"name\": \"User\"");
            var v2Schema = Schema.Parse(v2SchemaText);
            var v2Record = new GenericRecord((RecordSchema)v2Schema);
            v2Record.Add("id", "user456");
            v2Record.Add("name", "Jane Doe");
            v2Record.Add("active", true);
            v2Record.Add("status", "verified");
            serializer.Serialize(v2Record, _schemaName);

            // V3: Create record from existing user_v3.avsc but modify schema name to be consistent
            var v3SchemaText = File.ReadAllText(Path.Combine(assemblyDir, "../../../../../../shared/test/avro/backward/user_v3.avsc"));
            v3SchemaText = v3SchemaText.Replace("\"name\": \"user_v3\"", "\"name\": \"User\"");
            var v3Schema = Schema.Parse(v3SchemaText);
            var v3Record = new GenericRecord((RecordSchema)v3Schema);
            v3Record.Add("id", "user789");
            v3Record.Add("name", "Bob Smith");
            v3Record.Add("active", true);
            v3Record.Add("status", "premium");
            v3Record.Add("age", 35);
            serializer.Serialize(v3Record, _schemaName);

            // Verify 3 versions exist
            var versionsResponse = await _glueClient.ListSchemaVersionsAsync(new ListSchemaVersionsRequest
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
        }

        [Test]
        public async Task AvroBackwardEvolution_IncompatibleChange_ThrowsException()
        {
            var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
            var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/minimal-auto-registration-custom-registry.properties");
            configPath = Path.GetFullPath(configPath);

            var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

            // V1: Create record from existing employee_v1.avsc
            var v1SchemaPath = Path.Combine(assemblyDir, "../../../../../../shared/test/avro/negative/backward/employee_v1.avsc");
            var v1Schema = Schema.Parse(File.ReadAllText(v1SchemaPath));
            var v1Record = new GenericRecord((RecordSchema)v1Schema);
            v1Record.Add("id", 123);
            v1Record.Add("firstName", "John");
            v1Record.Add("lastName", "Doe");
            v1Record.Add("department", "Engineering");
            serializer.Serialize(v1Record, _schemaName);

            // Incompatible: Use employee_v2.avsc which adds required field (breaks backward compatibility)
            var incompatibleSchemaPath = Path.Combine(assemblyDir, "../../../../../../shared/test/avro/negative/backward/employee_v2.avsc");
            var incompatibleSchema = Schema.Parse(File.ReadAllText(incompatibleSchemaPath));
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
        }
    }
}
