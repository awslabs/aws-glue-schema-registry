using System;
using System.IO;
using System.Threading.Tasks;
using Amazon.Glue;
using Amazon.Glue.Model;
using NUnit.Framework;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.Tests.utils;

namespace AWSGsrSerDe.Tests.EvolutionTests
{
    [TestFixture]
    public class ProtobufEvolutionTests
    {
        private const string CUSTOM_REGISTRY_NAME = "native-test-registry";
        private IAmazonGlue _glueClient;
        private string _schemaName;

        [SetUp]
        public void SetUp()
        {
            _glueClient = new AmazonGlueClient();
            _schemaName = $"protobuf-evolution-test-{Guid.NewGuid():N}";
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
        public async Task ProtobufBackwardEvolution_RegistersVersionsSuccessfully()
        {
            var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
            var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/minimal-auto-registration-custom-registry.properties");
            configPath = Path.GetFullPath(configPath);

            var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

            // V1: Register base schema
            var v1Data = ProtobufGenerator.CreateUserV1();
            serializer.Serialize(v1Data, _schemaName);

            // V2: Add optional fields (backward compatible)
            var v2Data = ProtobufGenerator.CreateUserV2();
            serializer.Serialize(v2Data, _schemaName);

            // V3: Remove field + add optional (backward compatible)  
            var v3Data = ProtobufGenerator.CreateUserV3();
            serializer.Serialize(v3Data, _schemaName);

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
            Assert.That(versionsResponse.Schemas[0].VersionNumber, Is.EqualTo(1));
            Assert.That(versionsResponse.Schemas[1].VersionNumber, Is.EqualTo(2));
            Assert.That(versionsResponse.Schemas[2].VersionNumber, Is.EqualTo(3));
        }

        [Test]
        public async Task ProtobufBackwardEvolution_IncompatibleChange_ThrowsException()
        {
            var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
            var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/minimal-auto-registration-custom-registry.properties");
            configPath = Path.GetFullPath(configPath);

            var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

            // V1: Register base schema
            var v1Data = ProtobufGenerator.CreateUserV1();
            serializer.Serialize(v1Data, _schemaName);

            // Incompatible: Change field type (string email -> int32 email)
            var incompatibleData = ProtobufGenerator.CreateUserV1Incompatible();
            
            // Should throw exception due to backward incompatible change
            Assert.ThrowsAsync<AwsSchemaRegistryException>(async () =>
            {
                serializer.Serialize(incompatibleData, _schemaName);
            });
        }
    }
}
