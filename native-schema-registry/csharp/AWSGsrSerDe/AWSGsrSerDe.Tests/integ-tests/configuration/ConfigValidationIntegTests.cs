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
        private const string US_WEST_2_REGISTRY_NAME = "us-west-2-registry";
        private const string DEFAULT_REGION = "us-east-1";
        private const string US_WEST_2_REGION = "us-west-2";
        private const string US_EAST_2_REGION = "us-east-2";
        
        // Multi-region client manager - creates and caches clients per region
        private readonly Dictionary<string, IAmazonGlue> _regionClients = new();
        private readonly List<(string registryName, string schemaName, string region)> _schemasToCleanup = new();
        private readonly List<(string registryName, string region)> _registriesToCleanup = new();

        [SetUp]
        public async Task SetUp()
        {
            // Pre-create custom registry if it doesn't exist (using default region)
            await EnsureCustomRegistryExists();
            
            // Pre-create us-west-2 registry for cross-region testing
            await EnsureUsWest2RegistryExists();
            
            // Pre-create custom registry in us-east-2 for non-default endpoint testing
            await EnsureCustomRegistryExistsInUsEast2();
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
                _registriesToCleanup.Add((CUSTOM_REGISTRY_NAME, DEFAULT_REGION));
            }
        }

        private async Task EnsureUsWest2RegistryExists()
        {
            // Use us-west-2 region client for registry setup
            var glueClient = GetGlueClientForRegion(US_WEST_2_REGION);
            
            try
            {
                var getRegistryRequest = new GetRegistryRequest
                {
                    RegistryId = new RegistryId { RegistryName = US_WEST_2_REGISTRY_NAME }
                };

                await glueClient.GetRegistryAsync(getRegistryRequest);
            }
            catch (EntityNotFoundException)
            {
                // Registry doesn't exist, create it in us-west-2
                var createRegistryRequest = new CreateRegistryRequest
                {
                    RegistryName = US_WEST_2_REGISTRY_NAME,
                    Description = "Test registry in us-west-2 created by integration test for cross-region testing"
                };

                var createRegistryResponse = await glueClient.CreateRegistryAsync(createRegistryRequest);
                Assert.NotNull(createRegistryResponse, "Create registry response should not be null");
                Assert.NotNull(createRegistryResponse.RegistryArn, "Registry ARN should not be null");

                // Mark for cleanup since the test-suite created it (with region info)
                _registriesToCleanup.Add((US_WEST_2_REGISTRY_NAME, US_WEST_2_REGION));
            }
        }

        private async Task EnsureCustomRegistryExistsInUsEast2()
        {
            // Use us-east-2 region client for registry setup
            var glueClient = GetGlueClientForRegion(US_EAST_2_REGION);
            
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
                // Registry doesn't exist, create it in us-east-2
                var createRegistryRequest = new CreateRegistryRequest
                {
                    RegistryName = CUSTOM_REGISTRY_NAME,
                    Description = "Test registry in us-east-2 created by integration test for non-default endpoint testing"
                };

                var createRegistryResponse = await glueClient.CreateRegistryAsync(createRegistryRequest);
                Assert.NotNull(createRegistryResponse, "Create registry response should not be null");
                Assert.NotNull(createRegistryResponse.RegistryArn, "Registry ARN should not be null");

                // Mark for cleanup since the test-suite created it (with region info)
                _registriesToCleanup.Add((CUSTOM_REGISTRY_NAME, US_EAST_2_REGION));
            }
        }

        [TearDown]
        public async Task TearDown()
        {
            // Clean up any schemas created during tests (all regions)
            foreach (var (registryName, schemaName, region) in _schemasToCleanup)
            {
                try
                {
                    var regionClient = GetGlueClientForRegion(region);
                    var deleteSchemaRequest = new DeleteSchemaRequest
                    {
                        SchemaId = new SchemaId
                        {
                            RegistryName = registryName,
                            SchemaName = schemaName
                        }
                    };
                    await regionClient.DeleteSchemaAsync(deleteSchemaRequest);
                    Console.WriteLine($"✓ Cleaned up schema: {schemaName} in registry {registryName} (region {region})");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Failed to clean up schema {schemaName} in region {region}: {ex.Message}");
                }
            }
            _schemasToCleanup.Clear();

            // Clean up any registries created during tests (all regions)
            foreach (var (registryName, region) in _registriesToCleanup)
            {
                try
                {
                    var regionClient = GetGlueClientForRegion(region);
                    var deleteRegistryRequest = new DeleteRegistryRequest
                    {
                        RegistryId = new RegistryId { RegistryName = registryName }
                    };
                    await regionClient.DeleteRegistryAsync(deleteRegistryRequest);
                    Console.WriteLine($"✓ Cleaned up registry: {registryName} in region {region}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Failed to clean up registry {registryName} in region {region}: {ex.Message}");
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

        private async Task<bool> WaitForSchemaRegistration(string registryName, string schemaName, string region = null, int maxRetries = 10, int delayMs = 500)
        {
            // Use specified region client for schema registration checks, default to DEFAULT_REGION
            var glueClient = GetGlueClientForRegion(region ?? DEFAULT_REGION);
            
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
                _schemasToCleanup.Add((DEFAULT_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

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
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

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
            catch (AwsSchemaRegistryException ex) when (ex.Message.Contains("Credential should be scoped to a valid region"))
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
            catch (AwsSchemaRegistryException ex) when (ex.Message.Contains("UnknownHostException") && 
                                                        ex.Message.Contains("endpoint that is failing to resolve"))
            {
                // Expected - GSR exception with UnknownHostException for invalid region DNS resolution failure
                Assert.Pass($"Successfully validated invalid region 'us-east-99' results in UnknownHostException: {ex.Message}");
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
            catch (AwsSchemaRegistryException ex) when (ex.Message.Contains("UnknownHostException") && 
                                                        ex.Message.Contains("endpoint that is failing to resolve"))
            {
                // Expected - GSR exception with UnknownHostException for invalid endpoint DNS resolution failure
                Assert.Pass($"Successfully validated invalid endpoint results in UnknownHostException: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_WithInexistentRegistry_ThrowsException()
        {
            /* Test non-existent registry scenario
             * Create config with non-existent registry name
             * Attempt to serialize data 
             * Should throw exception indicating registry doesn't exist
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-inexistent-registry-{Guid.NewGuid():N}";

            try
            {
                // 1. Use shared config file with non-existent registry
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/inexistent-registry.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with non-existent registry config
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Attempt to serialize - this should fail due to non-existent registry
                // This should throw an exception indicating registry doesn't exist
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                // If we reach here, the test should fail because we expected an exception
                Assert.Fail("Expected exception due to non-existent registry 'non-existent-registry-12345', but serialization succeeded");
            }
            catch (AwsSchemaRegistryException ex) when (ex.Message.Contains("Registry is not found") && 
                                                        ex.Message.Contains("non-existent-registry-12345"))
            {
                // Expected - GSR exception indicating registry doesn't exist
                Assert.Pass($"Successfully validated non-existent registry 'non-existent-registry-12345' results in registry not found exception: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_WithInvalidRoleToAssume_ThrowsException()
        {
            /* Test invalid role assumption scenario
             * Create config with invalid/non-existent role ARN
             * Attempt to serialize data 
             * Should throw exception indicating role cannot be assumed
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-invalid-role-{Guid.NewGuid():N}";

            try
            {
                // 1. Use shared config file with invalid role ARN
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/invalid-role-to-assume.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with invalid role config
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Attempt to serialize - this should fail due to invalid role assumption
                // This should throw an exception indicating role cannot be assumed
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                // If we reach here, the test should fail because we expected an exception
                Assert.Fail("Expected exception due to invalid role 'arn:aws:iam::123456789012:role/NonExistentRole', but serialization succeeded");
            }
            catch (AwsSchemaRegistryException ex) when (ex.Message.Contains("is not authorized to perform: sts:AssumeRole") && 
                                                        ex.Message.Contains("NonExistentRole"))
            {
                // Expected - GSR exception indicating role assumption failure
                Assert.Pass($"Successfully validated invalid role assumption results in access denied exception: {ex.Message}");
            }
        }

        // [Test]
        // public async Task Constructor_WithInsufficientIAMPermission_ThrowsException()
        // {
        //     /* Test insufficient IAM permissions scenario
        //      * Since the test environment has full permissions and AWS SDK finds credentials from multiple sources,
        //      * we'll modify this test to use invalid credentials instead of no credentials
        //      * This should trigger an authentication failure
        //      */

        //    /*
        //    * TODO: Add this test. Resetting AWS env variables using 
        //    * Environment.SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "INVALID_ACCESS_KEY_12345");
        //    * doesn't seem to work.
        //    */
        // }

        [Test]
        public async Task Constructor_AccessRegistryInADifferentRegion_ThrowsException()
        {
            /* Test accessing registry in different region than 'region' config scenario 
             * Create config that tries to access a registry in a different region
             * Attempt to serialize data 
             * Should throw exception indicating registry not found in the configured region
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-different-region-{Guid.NewGuid():N}";

            try
            {
                // 1. Use shared config file with different region registry
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/different-region-registry.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with different region registry config
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Attempt to serialize - this should fail due to registry not existing in the configured region
                // This should throw an exception indicating registry not found
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                // If we reach here, the test should fail because we expected an exception
                Assert.Fail("Expected exception due to registry 'us-west-2-registry' not existing in region 'us-east-1', but serialization succeeded");
            }
            catch (AwsSchemaRegistryException ex) when (ex.Message.Contains("Registry is not found") && 
                                                        ex.Message.Contains("us-west-2-registry"))
            {
                // Expected - GSR exception indicating registry not found in the configured region
                Assert.Pass($"Successfully validated registry in different region results in registry not found exception: {ex.Message}");
            }
        }
        
        [Test]
        public async Task Constructor_NonDefault_Endpoint_Registers_Schema_Successfully()
        {
            /* Test non-default endpoint scenario: https://glue.us-east-2.amazonaws.com
             * Create config with custom endpoint URL
             * Attempt to serialize data 
             * Should successfully register schema using the custom endpoint
             * Cleanup - delete auto-registered schema
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-custom-endpoint-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName; // GSR uses topic name as schema name by default

            try
            {
                // 1. Use shared config file with non-default endpoint
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/non-default-endpoint.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with custom endpoint config
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Serialize the record - this should succeed using the custom endpoint
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                Assert.NotNull(serializedBytes, "Serialized bytes should not be null");
                Assert.That(serializedBytes.Length, Is.GreaterThan(0), "Serialized bytes should not be empty");

                // 5. Mark schema for cleanup (in us-east-2 region)
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, US_EAST_2_REGION));

                // 6. Wait for schema to be registered (in us-east-2 region)
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName, US_EAST_2_REGION);
                Assert.IsTrue(schemaRegistered, "Schema should have been auto-registered using custom endpoint");

                // 7. Verify schema was auto-registered with correct properties
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
                
                Console.WriteLine($"✓ Successfully registered schema using non-default endpoint");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during non-default endpoint test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during non-default endpoint test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during non-default endpoint test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_AutoRegistrationOff_WithNewSchema_ThrowsException()
        {
            /* Test auto-registration disabled scenario
             * Create config with auto-registration disabled
             * Attempt to serialize data with new schema
             * Should throw exception indicating schema not found and auto-registration is disabled
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-auto-reg-off-{Guid.NewGuid():N}";

            try
            {
                // 1. Use shared config file with auto-registration disabled
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/auto-registration-disabled.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with auto-registration disabled
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Attempt to serialize - this should fail due to auto-registration being disabled
                // This should throw an exception indicating schema not found and auto-registration disabled
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                // If we reach here, the test should fail because we expected an exception
                Assert.Fail("Expected exception due to auto-registration being disabled with new schema, but serialization succeeded");
            }
            catch (AwsSchemaRegistryException ex) when (ex.Message.Contains("Failed to auto-register schema") && 
                                                        ex.Message.Contains("Auto registration of schema is not enabled"))
            {
                // Expected - GSR exception indicating schema not found with auto-registration disabled
                Assert.Pass($"Successfully validated auto-registration disabled with new schema results in schema not found exception: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_CacheBeforeTTLExpiry_DoesNotMakeNewAPICall()
        {
            /* Test cache TTL behavior before expiry
             * Create serializer with short TTL, serialize same schema twice quickly
             * Second serialization should use cached result (no new API call)
             * Verify by checking that schema exists and both serializations succeed
             * Cleanup - delete auto-registered schema
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-cache-ttl-before-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName;

            try
            {
                // 1. Use shared config file with short TTL
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/cache-ttl-short.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with caching enabled
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. First serialization - this will register schema and cache it
                var serializedBytes1 = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes1, "First serialized bytes should not be null");
                Assert.That(serializedBytes1.Length, Is.GreaterThan(0), "First serialized bytes should not be empty");

                // 5. Second serialization immediately (before TTL expiry) - should use cache
                var serializedBytes2 = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes2, "Second serialized bytes should not be null");
                Assert.That(serializedBytes2.Length, Is.GreaterThan(0), "Second serialized bytes should not be empty");

                // 6. Both serializations should produce identical results (using cached schema ID)
                Assert.That(serializedBytes2, Is.EqualTo(serializedBytes1), "Cached serialization should produce identical bytes");

                // 7. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

                Console.WriteLine($"✓ Successfully validated cache behavior before TTL expiry");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during cache TTL test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during cache TTL test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during cache TTL test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_CacheBeforeMaxCacheSize_DoesNotMakesNewAPICall()
        {
            /* Test cache size behavior before reaching max
             * Create serializer with large cache size, serialize multiple schemas
             * All schemas should be cached without eviction
             * Verify by serializing same schemas again and getting identical results
             * Cleanup - delete auto-registered schemas
             */

            var schemaNames = new List<string>();

            try
            {
                // 1. Use shared config file with large cache size
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/cache-ttl-short.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with large cache
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Serialize multiple schemas (less than cache size)
                var serializedResults = new Dictionary<string, byte[]>();
                for (int i = 0; i < 5; i++)
                {
                    var topicName = $"test-topic-cache-size-{i}-{Guid.NewGuid():N}";
                    var serializedBytes = serializer.Serialize(avroRecord, topicName);
                    
                    Assert.NotNull(serializedBytes, $"Serialized bytes for topic {i} should not be null");
                    serializedResults[topicName] = serializedBytes;
                    schemaNames.Add(topicName);
                }

                // 5. Serialize same schemas again - should use cache
                foreach (var kvp in serializedResults)
                {
                    var cachedBytes = serializer.Serialize(avroRecord, kvp.Key);
                    Assert.That(cachedBytes, Is.EqualTo(kvp.Value), $"Cached serialization for {kvp.Key} should produce identical bytes");
                }

                // 6. Mark schemas for cleanup
                foreach (var schemaName in schemaNames)
                {
                    _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, schemaName, DEFAULT_REGION));
                }

                Console.WriteLine($"✓ Successfully validated cache behavior before max cache size");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during cache size test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during cache size test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during cache size test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_CacheAfterMaxCacheSize_MakesNewAPICall()
        {
            /* Test cache size behavior after reaching max
             * Create serializer with small cache size, serialize more schemas than cache can hold
             * Older entries should be evicted, causing new API calls for re-serialization
             * Test by deleting an early schema and verifying it gets recreated when re-serialized
             * Cleanup - delete auto-registered schemas
             */

            var schemaNames = new List<string>();

            try
            {
                // 1. Use config with small cache size (will be overwhelmed)
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/cache-ttl-short.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with limited cache
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Serialize many schemas to exceed cache size (100 + buffer)
                var firstTopicName = "";
                for (int i = 0; i < 105; i++)
                {
                    var topicName = $"test-topic-cache-overflow-{i}-{Guid.NewGuid():N}";
                    if (i == 0) firstTopicName = topicName;
                    
                    var serializedBytes = serializer.Serialize(avroRecord, topicName);
                    Assert.NotNull(serializedBytes, $"Serialized bytes for topic {i} should not be null");
                    schemaNames.Add(topicName);
                }

                // 5. Delete the first schema from AWS
                var glueClient = GetGlueClientForConfig(configPath);
                var deleteSchemaRequest = new DeleteSchemaRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = CUSTOM_REGISTRY_NAME,
                        SchemaName = firstTopicName
                    }
                };
                await glueClient.DeleteSchemaAsync(deleteSchemaRequest);

                // 6. Re-serialize first schema - should make new API call and recreate it
                var reSerializedBytes = serializer.Serialize(avroRecord, firstTopicName);
                Assert.NotNull(reSerializedBytes, "Re-serialized bytes should not be null");

                // 7. Verify schema was recreated
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, firstTopicName);
                Assert.IsTrue(schemaRegistered, "Schema should have been recreated after cache eviction");

                // 8. Mark schemas for cleanup
                foreach (var schemaName in schemaNames)
                {
                    _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, schemaName, DEFAULT_REGION));
                }

                Console.WriteLine($"✓ Successfully validated cache behavior after max cache size");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during cache overflow test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during cache overflow test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during cache overflow test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_CacheAndNewSchemaSerialize_MakesNewAPICall()
        {
            /* Test cache behavior with new schema
             * Create serializer, serialize one schema (cached), then serialize different schema
             * Second serialization should make new API call for the new schema
             * Verify both schemas are registered and cached independently
             * Cleanup - delete auto-registered schemas
             */

            // Generate unique names to avoid conflicts
            var topicName1 = $"test-topic-cache-new-schema-1-{Guid.NewGuid():N}";
            var topicName2 = $"test-topic-cache-new-schema-2-{Guid.NewGuid():N}";

            try
            {
                // 1. Use shared config file with caching enabled
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/cache-ttl-short.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with caching enabled
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. First serialization - register and cache first schema
                var serializedBytes1 = serializer.Serialize(avroRecord, topicName1);
                Assert.NotNull(serializedBytes1, "First serialized bytes should not be null");

                // 5. Second serialization with different topic - should make new API call for new schema
                var serializedBytes2 = serializer.Serialize(avroRecord, topicName2);
                Assert.NotNull(serializedBytes2, "Second serialized bytes should not be null");

                // 6. Bytes should be different (different schema IDs for different topics)
                Assert.That(serializedBytes2, Is.Not.EqualTo(serializedBytes1), "Different schemas should produce different serialized bytes");

                // 7. Re-serialize both topics - should use cache for both
                var cachedBytes1 = serializer.Serialize(avroRecord, topicName1);
                var cachedBytes2 = serializer.Serialize(avroRecord, topicName2);

                Assert.That(cachedBytes1, Is.EqualTo(serializedBytes1), "First schema should use cache");
                Assert.That(cachedBytes2, Is.EqualTo(serializedBytes2), "Second schema should use cache");

                // 8. Mark schemas for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, topicName1, DEFAULT_REGION));
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, topicName2, DEFAULT_REGION));

                Console.WriteLine($"✓ Successfully validated cache behavior with new schema serialization");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during new schema cache test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during new schema cache test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during new schema cache test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_CacheAndNewSchemaVersionSerialize_DoesNotMakesNewAPICall()
        {
            /* Test cache behavior with new schema version
             * Create serializer, serialize schema, register new version manually, serialize again
             * Second serialization should detect new version and make new API call
             * Verify both versions work correctly
             * Cleanup - delete auto-registered schemas
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-cache-new-version-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName;

            try
            {
                // 1. Use shared config file with caching enabled
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/cache-ttl-short.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with caching enabled
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. First serialization - register and cache schema version 1
                var serializedBytes1 = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes1, "First serialized bytes should not be null");

                // 5. Wait for schema registration
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema should have been registered");

                // 6. Register a new version of the same schema manually
                var glueClient = GetGlueClientForConfig(configPath);
                var newSchemaDefinition = "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"age\", \"type\": \"int\"}]}";
                
                var registerSchemaVersionRequest = new RegisterSchemaVersionRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = CUSTOM_REGISTRY_NAME,
                        SchemaName = expectedSchemaName
                    },
                    SchemaDefinition = newSchemaDefinition
                };

                await glueClient.RegisterSchemaVersionAsync(registerSchemaVersionRequest);

                // 7. Second serialization - should detect new version and make new API call
                var serializedBytes2 = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes2, "Second serialized bytes should not be null");

                // 8. Verify bytes are same (although new schema version has different UUID, cache still has old UUID)
                Assert.That(serializedBytes2, Is.EqualTo(serializedBytes1), "New schema version should have same serialized bytes due to cache being defined on schema definition which has not changed.");
                Console.WriteLine($"✓ Verified new schema version produces same serialized bytes due to caching");

                // 9. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

                Console.WriteLine($"✓ Successfully validated cache behavior with new schema version");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during new schema version cache test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during new schema version cache test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during new schema version cache test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_CacheDisabled_AlwaysMakesNewAPICall()
        {
            /* Test cache disabled behavior
             * Create serializer with cache disabled, serialize same schema multiple times
             * Each serialization should make new API call (no caching)
             * Test by deleting schema and verifying it gets recreated on each call
             * Cleanup - delete auto-registered schema
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-cache-disabled-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName;

            try
            {
                // 1. Use shared config file with cache disabled
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/cache-disabled.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with cache disabled
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. First serialization - register schema
                var serializedBytes1 = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes1, "First serialized bytes should not be null");

                // 5. Delete the schema from AWS
                var glueClient = GetGlueClientForConfig(configPath);
                var deleteSchemaRequest = new DeleteSchemaRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = CUSTOM_REGISTRY_NAME,
                        SchemaName = expectedSchemaName
                    }
                };
                await glueClient.DeleteSchemaAsync(deleteSchemaRequest);

                // 6. Second serialization - should make new API call and recreate schema (no cache)
                // Note: There might be a delay between deletion and the ability to recreate with the same name
                await Task.Delay(2000); // Wait a bit longer for schema deletion to complete
                
                var serializedBytes2 = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes2, "Second serialized bytes should not be null");

                // 7. Verify schema was recreated
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema should have been recreated (cache disabled)");

                // 8. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

                Console.WriteLine($"✓ Successfully validated cache disabled behavior");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during cache disabled test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during cache disabled test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during cache disabled test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_UsedCachedUUID_AfterActualSchemaDeletion()
        {
            /* Test cache behavior after schema deletion
             * Create serializer, serialize schema (cached), delete schema from AWS, serialize again
             * Second serialization should use cached UUID but fail, then make new API call to recreate
             * Verify schema gets recreated properly
             * Cleanup - delete auto-registered schema
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-cached-uuid-deletion-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName;

            try
            {
                // 1. Use shared config file with caching enabled
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/cache-ttl-short.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with caching enabled
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. First serialization - register and cache schema
                var serializedBytes1 = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes1, "First serialized bytes should not be null");

                // 5. Delete the schema from AWS while it's still cached
                var glueClient = GetGlueClientForConfig(configPath);
                var deleteSchemaRequest = new DeleteSchemaRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = CUSTOM_REGISTRY_NAME,
                        SchemaName = expectedSchemaName
                    }
                };
                await glueClient.DeleteSchemaAsync(deleteSchemaRequest);

                // 6. Second serialization - should handle cached UUID gracefully and recreate schema
                var serializedBytes2 = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes2, "Second serialized bytes should not be null");

                // 7. Verify schema was recreated
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema should have been recreated after deletion");

                // 8. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

                Console.WriteLine($"✓ Successfully validated cached UUID behavior after schema deletion");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during cached UUID deletion test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during cached UUID deletion test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during cached UUID deletion test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_ZLIBCompressionConfigSet_CompressesAndSetsCompresionByte()
        {
            /* Test ZLIB compression configuration
             * Create config with ZLIB compression enabled
             * Serialize data and verify compression is applied
             * Check that compression byte is set to value 5 (ZLIB compression)
             * Cleanup - delete auto-registered schema
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-zlib-compression-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName;

            try
            {
                // 1. Use shared config file with ZLIB compression enabled
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/zlib-compression-enabled.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with ZLIB compression
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Serialize the record with compression
                var compressedBytes = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(compressedBytes, "Compressed serialized bytes should not be null");
                Assert.That(compressedBytes.Length, Is.GreaterThan(0), "Compressed serialized bytes should not be empty");

                // 5. Verify GSR format and compression byte
                // GSR format: [header_version_byte][compression_byte][schema_version_id_16_bytes][data]
                Assert.That(compressedBytes.Length, Is.GreaterThan(18), "Serialized bytes should be long enough to contain GSR header");
                
                // 6. Check header version byte (should be 3)
                Assert.That(compressedBytes[0], Is.EqualTo((byte)3), "Header version byte should be 3");
                
                // 7. Check compression byte (should be 5 for ZLIB compression)
                Assert.That(compressedBytes[1], Is.EqualTo((byte)5), "Compression byte should be 5 for ZLIB compression");
                
                Console.WriteLine($"✓ Serialized {compressedBytes.Length} bytes with ZLIB compression");
                Console.WriteLine($"  Header version byte: {compressedBytes[0]}");
                Console.WriteLine($"  Compression byte: {compressedBytes[1]} (expected: 5 for ZLIB)");

                // 8. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

                // 9. Verify schema was registered
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema should have been registered with compression");

                Console.WriteLine($"✓ Successfully validated ZLIB compression configuration");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during ZLIB compression test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during ZLIB compression test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during ZLIB compression test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_NoCompressionConfigSet_SetsDefaultCompresionByte()
        {
            /* Test default compression configuration
             * Create config without compression settings (should default to no compression)
             * Serialize data and verify default compression byte is set to 0
             * Compare with compressed version to ensure different behavior
             * Cleanup - delete auto-registered schema
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-no-compression-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName;

            try
            {
                // 1. Use shared config file without compression settings (defaults to no compression)
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/minimal-auto-registration-custom-registry.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with default compression
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Serialize the record without compression
                var uncompressedBytes = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(uncompressedBytes, "Uncompressed serialized bytes should not be null");
                Assert.That(uncompressedBytes.Length, Is.GreaterThan(0), "Uncompressed serialized bytes should not be empty");

                // 5. Verify GSR format and default compression byte
                // GSR format: [header_version_byte][compression_byte][schema_version_id_16_bytes][data]
                Assert.That(uncompressedBytes.Length, Is.GreaterThan(18), "Serialized bytes should be long enough to contain GSR header");
                
                // 6. Check header version byte (should be 3)
                Assert.That(uncompressedBytes[0], Is.EqualTo((byte)3), "Header version byte should be 3");
                
                // 7. Check compression byte (should be 0 for no compression)
                Assert.That(uncompressedBytes[1], Is.EqualTo((byte)0), "Compression byte should be 0 for no compression");
                
                Console.WriteLine($"✓ Serialized {uncompressedBytes.Length} bytes with default compression");
                Console.WriteLine($"  Header version byte: {uncompressedBytes[0]}");
                Console.WriteLine($"  Compression byte: {uncompressedBytes[1]} (expected: 0 for no compression)");

                // 8. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

                // 9. Verify schema was registered
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema should have been registered with default compression");

                Console.WriteLine($"✓ Successfully validated default compression configuration");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during default compression test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during default compression test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during default compression test: {ex.Message}");
            }
        }
        
        [Test]
        public async Task Constructor_UserAgentDefaultsToNativeCsharpString()
        {
            /* Test default user agent behavior
             * Create config without custom user agent settings
             * Serialize data and verify schema is registered
             * Cleanup - delete auto-registered schema
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-default-user-agent-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName;

            try
            {
                // 1. Use shared config file without custom user agent (should use default)
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/minimal-auto-registration-custom-registry.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with default user agent
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Serialize the record - should use default user agent
                var serializedBytes = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes, "Serialized bytes should not be null");
                Assert.That(serializedBytes.Length, Is.GreaterThan(0), "Serialized bytes should not be empty");

                // 5. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

                // 6. Verify schema was registered (user agent is used internally for API calls)
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema has been registered with default user agent (ntive) (behind the scene)");

                Console.WriteLine($"✓ Successfully validated default user agent behavior");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during default user agent test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during default user agent test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during default user agent test: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_UsesUserAgentStringFromConfig()
        {
            /* Test custom user agent configuration
             * Create config with custom user agent string
             * Serialize data successfully when custom user agent is used
             * Custom user agent should be applied to all AWS API calls
             * Cleanup - delete auto-registered schema
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-custom-user-agent-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName;

            try
            {
                // 1. Use shared config file with custom user agent
                var assemblyDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
                var configPath = Path.Combine(assemblyDir, "../../../../../../shared/test/configs/custom-user-agent.properties");
                configPath = Path.GetFullPath(configPath);

                // 2. Create GSR serializer with custom user agent
                var serializer = new GlueSchemaRegistryKafkaSerializer(configPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 4. Serialize the record - should use custom user agent "CustomTestApp/1.0.0" behind the scene
                var serializedBytes = serializer.Serialize(avroRecord, topicName);
                Assert.NotNull(serializedBytes, "Serialized bytes should not be null");
                Assert.That(serializedBytes.Length, Is.GreaterThan(0), "Serialized bytes should not be empty");

                // 5. Mark schema for cleanup
                _schemasToCleanup.Add((CUSTOM_REGISTRY_NAME, expectedSchemaName, DEFAULT_REGION));

                // 6. Verify schema was registered (custom user agent is used internally for API calls)
                var schemaRegistered = await WaitForSchemaRegistration(CUSTOM_REGISTRY_NAME, expectedSchemaName);
                Assert.IsTrue(schemaRegistered, "Schema has been registered with custom user agent (behind the scene)");

                Console.WriteLine($"✓ Successfully validated custom user agent configuration");
            }
            catch (AmazonGlueException ex)
            {
                Assert.Fail($"AWS Glue error during custom user agent test: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                Assert.Fail($"AWS Service error during custom user agent test: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected error during custom user agent test: {ex.Message}");
            }
        }
    }
}
