using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Glue;
using Amazon.Glue.Model;
using Amazon.Runtime;
using NUnit.Framework;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.Tests.utils;

namespace AWSGsrSerDe.Tests.Configuration
{
    [TestFixture]
    public class ConfigValidationIntegTests
    {
        private const string DEFAULT_REGISTRY_NAME = "default-registry";
        private const string CUSTOM_REGISTRY_NAME = "native-test-registry";
        private const string DEFAULT_REGION = "us-east-1";
        
        // Multi-region client manager - creates and caches clients per region
        private readonly Dictionary<string, IAmazonGlue> _regionClients = new();
        private readonly List<(string registryName, string schemaName)> _schemasToCleanup = new();
        private readonly List<string> _registriesToCleanup = new();

        [SetUp]
        public async Task SetUp()
        {
            // Pre-create custom registry if it doesn't exist (using default region)
            await EnsureCustomRegistryExists();
        }

        /// <summary>
        /// Gets or creates a Glue client for the specified region.
        /// Caches clients to avoid recreating them for the same region.
        /// </summary>
        private IAmazonGlue GetGlueClientForRegion(string region)
        {
            if (string.IsNullOrWhiteSpace(region))
            {
                region = DEFAULT_REGION;
            }

            if (!_regionClients.ContainsKey(region))
            {
                try
                {
                    var regionEndpoint = RegionEndpoint.GetBySystemName(region);
                    _regionClients[region] = new AmazonGlueClient(regionEndpoint);
                }
                catch (ArgumentException ex)
                {
                    throw new ArgumentException($"Invalid region '{region}' specified", ex);
                }
            }
            
            return _regionClients[region];
        }

        /// <summary>
        /// Gets a Glue client for the region specified in the config file.
        /// Falls back to default region if no region is specified in config.
        /// </summary>
        private IAmazonGlue GetGlueClientForConfig(string configPath)
        {
            var region = ExtractRegionFromConfig(configPath);
            return GetGlueClientForRegion(region);
        }

        /// <summary>
        /// Extracts the region from a config file, returns default region if not found.
        /// </summary>
        private string ExtractRegionFromConfig(string configPath)
        {
            try
            {
                if (!File.Exists(configPath))
                {
                    return DEFAULT_REGION;
                }

                var configLines = File.ReadAllLines(configPath);
                var regionLine = configLines.FirstOrDefault(line => line.StartsWith("region="));
                
                if (regionLine != null)
                {
                    var region = regionLine.Split('=')[1].Trim();
                    return string.IsNullOrWhiteSpace(region) ? DEFAULT_REGION : region;
                }
                
                return DEFAULT_REGION;
            }
            catch (Exception)
            {
                return DEFAULT_REGION;
            }
        }

        private async Task EnsureCustomRegistryExists()
        {
            // Use default region client for registry setup
            var glueClient = GetGlueClientForRegion(DEFAULT_REGION);
            
            try
            {
                var getRegistryRequest = new GetRegistryRequest
                {
                    RegistryId = new RegistryId { RegistryName = CUSTOM_REGISTRY_NAME }
                };

                await glueClient.GetRegistryAsync(getRegistryRequest);
            }
            catch (EntityNotFoundException)
            {
                // Registry doesn't exist, create it
                var createRegistryRequest = new CreateRegistryRequest
                {
                    RegistryName = CUSTOM_REGISTRY_NAME,
                    Description = "Test registry created by integration test"
                };

                var createRegistryResponse = await glueClient.CreateRegistryAsync(createRegistryRequest);
                Assert.NotNull(createRegistryResponse, "Create registry response should not be null");
                Assert.NotNull(createRegistryResponse.RegistryArn, "Registry ARN should not be null");

                // Mark for cleanup since the test-suite created it
                _registriesToCleanup.Add(CUSTOM_REGISTRY_NAME);
            }
        }

        [TearDown]
        public async Task TearDown()
        {
            // Use default region client for cleanup operations
            var glueClient = GetGlueClientForRegion(DEFAULT_REGION);
            
            // Clean up any schemas created during tests
            foreach (var (registryName, schemaName) in _schemasToCleanup)
            {
                try
                {
                    var deleteSchemaRequest = new DeleteSchemaRequest
                    {
                        SchemaId = new SchemaId
                        {
                            RegistryName = registryName,
                            SchemaName = schemaName
                        }
                    };
                    await glueClient.DeleteSchemaAsync(deleteSchemaRequest);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Failed to clean up schema {schemaName}: {ex.Message}");
                }
            }
            _schemasToCleanup.Clear();

            // Clean up any registries created during tests
            foreach (var registryName in _registriesToCleanup)
            {
                try
                {
                    var deleteRegistryRequest = new DeleteRegistryRequest
                    {
                        RegistryId = new RegistryId { RegistryName = registryName }
                    };
                    await glueClient.DeleteRegistryAsync(deleteRegistryRequest);
                    Console.WriteLine($"✓ Cleaned up registry: {registryName}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Failed to clean up registry {registryName}: {ex.Message}");
                }
            }
            _registriesToCleanup.Clear();

            // Dispose all region clients
            foreach (var client in _regionClients.Values)
            {
                client?.Dispose();
            }
            _regionClients.Clear();
        }

        private async Task<bool> WaitForSchemaRegistration(string registryName, string schemaName, int maxRetries = 10, int delayMs = 500)
        {
            // Use default region client for schema registration checks
            var glueClient = GetGlueClientForRegion(DEFAULT_REGION);
            
            for (int i = 0; i < maxRetries; i++)
            {
                try
                {
                    var getSchemaRequest = new GetSchemaRequest
                    {
                        SchemaId = new SchemaId
                        {
                            RegistryName = registryName,
                            SchemaName = schemaName
                        }
                    };
                    
                    var response = await glueClient.GetSchemaAsync(getSchemaRequest);
                    if (response != null && response.SchemaName == schemaName)
                    {
                        return true;
                    }
                }
                catch (EntityNotFoundException)
                {
                    // Schema not found yet, wait and retry
                    await Task.Delay(delayMs);
                }
            }
            return false;
        }


        [Test]
        public async Task Constructor_WithValidMinimalConfig_AutoRegistersSchemaInDefaultRegistry()
        {
            /* Use GSR serializer with auto-registration enabled to serialize data
             * This should automatically register a schema in the default registry when no registry is specified
             * Verify schema was auto-registered
             * Cleanup - delete auto-registered schema and temp config file
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName; // GSR uses topic name as schema name by default
            string configPath = null;

            try
            {
                // 1. Use shared config file with auto-registration enabled and no registry specification
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/minimal-auto-registration-default-registry.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with auto-registration enabled
                Console.WriteLine($"Creating GSR serializer with config...");
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();
                Console.WriteLine($"✓ Created test Avro record: {avroRecord}");

                // 4. Serialize the record - this should auto-register the schema
                Console.WriteLine($"Serializing record to topic '{topicName}' (should auto-register schema)...");
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                Assert.NotNull(serializedBytes, "Serialized bytes should not be null");
                Assert.That(serializedBytes.Length, Is.GreaterThan(0), "Serialized bytes should not be empty");
                Console.WriteLine($"✓ Successfully serialized record, {serializedBytes.Length} bytes");

                // 5. Mark schema for cleanup
                _schemasToCleanup.Add((DEFAULT_REGISTRY_NAME, expectedSchemaName));

                // 6. Wait for schema to be registered
                Console.WriteLine("Waiting for schema registration to complete...");
                var schemaRegistered = await WaitForSchemaRegistration(DEFAULT_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema should have been auto-registered");

                // 7. Verify schema was auto-registered with correct properties
                var glueClient = GetGlueClientForConfig(configPath);
                var getSchemaRequest = new GetSchemaRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = DEFAULT_REGISTRY_NAME,
                        SchemaName = expectedSchemaName
                    }
                };

                var getResponse = await glueClient.GetSchemaAsync(getSchemaRequest);

                Assert.NotNull(getResponse, "Get schema response should not be null");
                Assert.That(getResponse.SchemaName, Is.EqualTo(expectedSchemaName), "Auto-registered schema name should match topic name");
                Assert.That(getResponse.DataFormat, Is.EqualTo(DataFormat.AVRO), "Auto-registered schema should be AVRO format");
                Console.WriteLine($"✓ Verified schema was auto-registered in default registry");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during auto-registration test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during auto-registration test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during auto-registration test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_WithValidMinimalConfig_AutoRegistersSchemaInCustomRegistry()
        {
            /* Use GSR serializer with auto-registration enabled to serialize data
             * This should automatically register a schema in the custom registry
             * Custom registry is pre-created in SetUp method
             * Verify schema was auto-registered in custom registry
             * Cleanup handled by TearDown method
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-custom-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName; // GSR uses topic name as schema name by default

            try
            {
                // 1. Use shared config file with auto-registration enabled for custom registry (native-test-registry)
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/minimal-auto-registration-custom-registry.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with auto-registration enabled for custom registry
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Serialize the record - this should auto-register the schema in custom registry
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                Assert.NotNull(serializedBytes, "Serialized bytes should not be null");
                Assert.That(serializedBytes.Length, Is.GreaterThan(0), "Serialized bytes should not be empty");

                // 5. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName));

                // 6. Wait for schema to be registered
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema should have been auto-registered in custom registry");

                // 7. Verify schema was auto-registered in custom registry
                var glueClient = GetGlueClientForConfig(configPath);
                var getSchemaRequest = new GetSchemaRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = CUSTOM_REGISTRY_NAME,
                        SchemaName = expectedSchemaName
                    }
                };

                var getResponse = await glueClient.GetSchemaAsync(getSchemaRequest);

                Assert.NotNull(getResponse, "Get schema response should not be null");
                Assert.That(getResponse.SchemaName, Is.EqualTo(expectedSchemaName), "Auto-registered schema name should match topic name");
                Assert.That(getResponse.DataFormat, Is.EqualTo(DataFormat.AVRO), "Auto-registered schema should be AVRO format");
                Assert.That(getResponse.RegistryName, Is.EqualTo(CUSTOM_REGISTRY_NAME), "Schema should be in the custom registry");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during custom registry auto-registration test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during custom registry auto-registration test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during custom registry auto-registration test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_WithRegionEndpointMismatch_ThrowsAccessDeniedException()
        {
            /* Test region/endpoint mismatch scenario
             * Create config with mismatched region and endpoint
             * Attempt to serialize data 
             * Should throw AccessDenied exception as the IAM permissions for the configured region will not apply to the endpoint's region
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-mismatch-{Guid.NewGuid():N}";
            string configPath = null;

            try
            {
                // 1. Use shared config file with mismatched region and endpoint
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/region-endpoint-mismatch.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with mismatched config
                TestContext.WriteLine("Creating GSR serializer with mismatched region/endpoint config...");
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Attempt to serialize - this should fail due to region/endpoint mismatch
                // This should throw an access denied error - Credential should be scoped to a valid region. (Service: Glue, Status Code: 400, Request ID)
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                // If we reach here, the test should fail because we expected an exception
                Assert.Fail("Expected AccessDenied or authorization exception due to IAM permissions in different region, but serialization succeeded");
            }
            catch (AwsSchemaRegistryException ex) when (ex.GetType().Name == "AwsSchemaRegistryException" &&
                                      (ex.Message.Contains("Credential should be scoped to a valid region") ||
                                       ex.Message.Contains("scoped to a valid region")))
            {
                // Expected - this is exactly what we want to test for region/endpoint mismatch
                Assert.Pass($"Successfully validated region/endpoint mismatch results in credential scoping error: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_WithInvalidRegion_ThrowsException()
        {
            /* Test invalid region scenario
             * Create config with invalid region (us-east-99)
             * Attempt to serialize data 
             * Should throw exception indicating invalid region
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-invalid-region-{Guid.NewGuid():N}";
            string configPath = null;

            try
            {
                // 1. Use shared config file with invalid region
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/invalid-region.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with invalid region config
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Attempt to serialize - this should fail due to invalid region
                // This should throw an exception indicating invalid region
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                // If we reach here, the test should fail because we expected an exception
                Assert.Fail("Expected exception due to invalid region 'us-east-99', but serialization succeeded");
            }
            catch (Exception ex) when (ex.GetType().Name == "AwsSchemaRegistryException" &&
                                      ex.Message.Contains("UnknownHostException") &&
                                      ex.Message.Contains("endpoint that is failing to resolve"))
            {
                // Expected - GSR exception with UnknownHostException for invalid region DNS resolution failure
                Assert.Pass($"Successfully validated invalid region 'us-east-99' results in UnknownHostException");
            }
        }

        [Test]
        public async Task Constructor_WithInvalidEndpoint_ThrowsException()
        {
            /* Test invalid endpoint scenario
             * Create config with invalid endpoint URL
             * Attempt to serialize data 
             * Should throw exception indicating endpoint connection failure
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-invalid-endpoint-{Guid.NewGuid():N}";
            string configPath = null;

            try
            {
                // 1. Use shared config file with invalid endpoint
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/invalid-endpoint.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with invalid endpoint config
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Attempt to serialize - this should fail due to invalid endpoint
                // This should throw an exception indicating connection failure to invalid endpoint
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                // If we reach here, the test should fail because we expected an exception
                Assert.Fail("Expected exception due to invalid endpoint 'https://invalid-endpoint.amazonaws.com', but serialization succeeded");
            }
            catch (Exception ex) when (ex.GetType().Name == "AwsSchemaRegistryException" &&
                                      ex.Message.Contains("UnknownHostException") &&
                                      ex.Message.Contains("endpoint that is failing to resolve"))
            {
                // Expected - GSR exception with UnknownHostException for invalid endpoint DNS resolution failure
                Assert.Pass($"Successfully validated invalid endpoint 'https://invalid-endpoint.amazonaws.com' results in UnknownHostException");
            }
        }
    }
}
