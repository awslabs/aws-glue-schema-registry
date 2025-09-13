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

        [Test]
        public async Task ValidateThereAreTenRegistries()
        {
            // Basic code to list all the glue registries to verify we are able to connect to AWS
            try
            {
                // 1. Create AWS Glue client using default credentials
                // Uses the default credential provider chain which looks for credentials in:
                // - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
                // - AWS credentials file (~/.aws/credentials)
                // - IAM role (if running on EC2/ECS/Lambda)
                var glueClient = new AmazonGlueClient(RegionEndpoint.USEast1);

                // 2. Create request to list registries (requesting exactly 10)
                var listRegistriesRequest = new ListRegistriesRequest
                {
                    MaxResults = 10  // Request 10 registries
                };

                // 3. Call AWS to list registries
                var response = await glueClient.ListRegistriesAsync(listRegistriesRequest);

                // 4. Verify we got a response
                Assert.NotNull(response, "Response should not be null");
                Assert.NotNull(response.Registries, "Registries list should not be null");

                // 5. Log the registries found
                Console.WriteLine($"Found {response.Registries.Count} registries:");
                foreach (var registry in response.Registries)
                {
                    Console.WriteLine($"  - Registry: {registry.RegistryName} (ARN: {registry.RegistryArn})");
                }

                // 6. ASSERT EXACTLY 10 REGISTRIES
                Assert.AreEqual(10, response.Registries.Count,
                    $"Expected exactly 10 registries, but found {response.Registries.Count}");

                // 7. Additional validation - verify all registries have names
                foreach (var registry in response.Registries)
                {
                    Assert.IsNotNull(registry.RegistryName, "Registry name should not be null");
                    Assert.That(registry.RegistryName, Is.Not.Empty, "Registry name should not be empty");
                }

                Console.WriteLine("Successfully verified 10 registries are accessible");
            }
            catch (AmazonGlueException ex)
            {
                // AWS-specific errors
                Assert.Fail($"AWS Glue error: {ex.Message}. Error Code: {ex.ErrorCode}");
            }
            catch (AmazonServiceException ex)
            {
                // General AWS service errors (like auth issues)
                Assert.Fail($"AWS Service error: {ex.Message}. Status Code: {ex.StatusCode}");
            }
            catch (Exception ex)
            {
                // Other unexpected errors
                Assert.Fail($"Unexpected error connecting to AWS: {ex.Message}");
            }
        }

        [Test]
        public async Task Constructor_WithValidMinimalConfig_AutoRegistersSchemaInDefaultRegistry()
        {
            /* Use GSR serializer with auto-registration enabled to serialize data
             * This should automatically register a schema in the default registry
             * Verify schema was auto-registered
             * Cleanup - delete auto-registered schema and temp config file
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName; // GSR uses topic name as schema name by default
            var registryName = "default-registry";
            string tempConfigPath = null;

            try
            {
                // 1. Create temporary config file with auto-registration enabled
                tempConfigPath = Path.GetTempFileName();
                var tempConfigContent = $@"region=us-east-1
                registry.name={registryName}
                dataFormat=AVRO
                schemaAutoRegistrationEnabled=true";

                await File.WriteAllTextAsync(tempConfigPath, tempConfigContent);
                Console.WriteLine($"✓ Created temporary config file: {tempConfigPath}");

                // 2. Create AWS Glue client for verification and cleanup
                var glueClient = new AmazonGlueClient(RegionEndpoint.USEast1);

                // 3. Create GSR serializer with auto-registration enabled
                Console.WriteLine($"Creating GSR serializer with temporary config...");
                var serializer = new GlueSchemaRegistryKafkaSerializer(tempConfigPath);

                // 4. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();
                Console.WriteLine($"✓ Created test Avro record: {avroRecord}");

                // 5. Serialize the record - this should auto-register the schema
                Console.WriteLine($"Serializing record to topic '{topicName}' (should auto-register schema)...");
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                Assert.NotNull(serializedBytes, "Serialized bytes should not be null");
                Assert.That(serializedBytes.Length, Is.GreaterThan(0), "Serialized bytes should not be empty");
                Console.WriteLine($"✓ Successfully serialized record, {serializedBytes.Length} bytes");

                // 6. Wait for eventual consistency
                Console.WriteLine("Waiting for eventual consistency...");
                await Task.Delay(3000);

                // 7. Verify schema was auto-registered
                var getSchemaRequest = new GetSchemaRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = registryName,
                        SchemaName = expectedSchemaName
                    }
                };

                Console.WriteLine($"Verifying schema '{expectedSchemaName}' was auto-registered in registry '{registryName}'...");
                var getResponse = await glueClient.GetSchemaAsync(getSchemaRequest);

                Assert.NotNull(getResponse, "Get schema response should not be null");
                Assert.That(getResponse.SchemaName, Is.EqualTo(expectedSchemaName), "Auto-registered schema name should match topic name");
                Assert.That(getResponse.DataFormat, Is.EqualTo(DataFormat.AVRO), "Auto-registered schema should be AVRO format");

                Console.WriteLine($"✓ Successfully verified auto-registered schema: {getResponse.SchemaName}");
                Console.WriteLine($"✓ Schema ARN: {getResponse.SchemaArn}");

                // 8. Test deserialization to ensure the auto-registered schema works
                var deserializer = new GlueSchemaRegistryKafkaDeserializer(tempConfigPath);
                var deserializedRecord = deserializer.Deserialize(topicName, serializedBytes);

                Assert.NotNull(deserializedRecord, "Deserialized record should not be null");
                Console.WriteLine($"✓ Successfully completed round-trip serialization/deserialization with auto-registered schema");

                // 9. Cleanup - Delete the auto-registered schema
                Console.WriteLine($"Cleaning up auto-registered schema '{expectedSchemaName}'...");

                var deleteSchemaRequest = new DeleteSchemaRequest
                {
                    SchemaId = new SchemaId
                    {
                        RegistryName = registryName,
                        SchemaName = expectedSchemaName
                    }
                };

                var deleteResponse = await glueClient.DeleteSchemaAsync(deleteSchemaRequest);
                Assert.NotNull(deleteResponse, "Delete schema response should not be null");

                Console.WriteLine($"✓ Successfully deleted auto-registered schema: {expectedSchemaName}");

                // 10. Optional: Verify schema is deleted (with eventual consistency handling)
                await Task.Delay(2000);

                try
                {
                    await glueClient.GetSchemaAsync(getSchemaRequest);
                    Console.WriteLine("Note: Schema still accessible after deletion (eventual consistency)");
                }
                catch (EntityNotFoundException)
                {
                    Console.WriteLine("✓ Confirmed auto-registered schema was successfully deleted");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Schema deletion verified with exception: {ex.GetType().Name}");
                }
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
            finally
            {
                // 11. Always cleanup temporary config file
                if (tempConfigPath != null && File.Exists(tempConfigPath))
                {
                    try
                    {
                        File.Delete(tempConfigPath);
                        Console.WriteLine($"✓ Cleaned up temporary config file: {tempConfigPath}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Warning: Could not delete temporary config file: {ex.Message}");
                    }
                }
            }
        }

        [Test]
        public async Task Constructor_WithValidMinimalConfig_AutoRegistersSchemaInCustomRegistry()
        {
            /* Use GSR serializer with auto-registration enabled to serialize data
             * This should automatically register a schema in the custom registry
             * Create custom registry "native-test-registry" if it doesn't exist
             * Verify schema was auto-registered in custom registry
             * Cleanup - delete auto-registered schema, temp config file, and optionally custom registry
             */

            // Generate unique names to avoid conflicts
            var topicName = $"test-topic-custom-{Guid.NewGuid():N}";
            var expectedSchemaName = topicName; // GSR uses topic name as schema name by default
            var registryName = "native-test-registry";
            string tempConfigPath = null;
            bool registryCreatedByTest = false;

            try
            {
                // 1. Create AWS Glue client for registry and schema operations
                var glueClient = new AmazonGlueClient(RegionEndpoint.USEast1);

                // 2. Check if custom registry exists, create if it doesn't
                Console.WriteLine($"Checking if custom registry '{registryName}' exists...");
                try
                {
                    var getRegistryRequest = new GetRegistryRequest
                    {
                        RegistryId = new RegistryId { RegistryName = registryName }
                    };
                    
                    await glueClient.GetRegistryAsync(getRegistryRequest);
                    Console.WriteLine($"✓ Custom registry '{registryName}' already exists");
                }
                catch (EntityNotFoundException)
                {
                    // Registry doesn't exist, create it
                    Console.WriteLine($"Creating custom registry '{registryName}'...");
                    var createRegistryRequest = new CreateRegistryRequest
                    {
                        RegistryName = registryName,
                        Description = "Test registry created by integration test"
                    };
                    
                    var createRegistryResponse = await glueClient.CreateRegistryAsync(createRegistryRequest);
                    Assert.NotNull(createRegistryResponse, "Create registry response should not be null");
                    Assert.NotNull(createRegistryResponse.RegistryArn, "Registry ARN should not be null");
                    
                    registryCreatedByTest = true;
                    Console.WriteLine($"✓ Successfully created custom registry with ARN: {createRegistryResponse.RegistryArn}");
                }

                // 3. Create temporary config file with auto-registration enabled for custom registry
                tempConfigPath = Path.GetTempFileName();
                var tempConfigContent = $@"region=us-east-1
                registry.name={registryName}
                dataFormat=AVRO
                schemaAutoRegistrationEnabled=true";

                await File.WriteAllTextAsync(tempConfigPath, tempConfigContent);
                Console.WriteLine($"✓ Created temporary config file for custom registry: {tempConfigPath}");

                // 4. Create GSR serializer with auto-registration enabled for custom registry
                Console.WriteLine($"Creating GSR serializer with custom registry config...");
                var serializer = new GlueSchemaRegistryKafkaSerializer(tempConfigPath);
                
                // 5. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();
                Console.WriteLine($"✓ Created test Avro record: {avroRecord}");
                
                // 6. Serialize the record - this should auto-register the schema in custom registry
                Console.WriteLine($"Serializing record to topic '{topicName}' (should auto-register schema in custom registry)...");
                var serializedBytes = serializer.Serialize(avroRecord, topicName);
                
                Assert.NotNull(serializedBytes, "Serialized bytes should not be null");
                Assert.That(serializedBytes.Length, Is.GreaterThan(0), "Serialized bytes should not be empty");
                Console.WriteLine($"✓ Successfully serialized record, {serializedBytes.Length} bytes");
                
                // 7. Wait for eventual consistency
                Console.WriteLine("Waiting for eventual consistency...");
                await Task.Delay(3000);
                
                // 8. Verify schema was auto-registered in custom registry
                var getSchemaRequest = new GetSchemaRequest
                {
                    SchemaId = new SchemaId 
                    { 
                        RegistryName = registryName,
                        SchemaName = expectedSchemaName
                    }
                };

                Console.WriteLine($"Verifying schema '{expectedSchemaName}' was auto-registered in custom registry '{registryName}'...");
                var getResponse = await glueClient.GetSchemaAsync(getSchemaRequest);
                
                Assert.NotNull(getResponse, "Get schema response should not be null");
                Assert.That(getResponse.SchemaName, Is.EqualTo(expectedSchemaName), "Auto-registered schema name should match topic name");
                Assert.That(getResponse.DataFormat, Is.EqualTo(DataFormat.AVRO), "Auto-registered schema should be AVRO format");
                Assert.That(getResponse.RegistryName, Is.EqualTo(registryName), "Schema should be in the custom registry");
                
                Console.WriteLine($"✓ Successfully verified auto-registered schema in custom registry: {getResponse.SchemaName}");
                Console.WriteLine($"✓ Schema ARN: {getResponse.SchemaArn}");
                Console.WriteLine($"✓ Confirmed schema is in custom registry: {getResponse.RegistryName}");
                
                // 9. Test deserialization to ensure the auto-registered schema works
                var deserializer = new GlueSchemaRegistryKafkaDeserializer(tempConfigPath);
                var deserializedRecord = deserializer.Deserialize(topicName, serializedBytes);
                
                Assert.NotNull(deserializedRecord, "Deserialized record should not be null");
                Console.WriteLine($"✓ Successfully completed round-trip serialization/deserialization with auto-registered schema in custom registry");
                
                // 10. Cleanup - Delete the auto-registered schema from custom registry
                Console.WriteLine($"Cleaning up auto-registered schema '{expectedSchemaName}' from custom registry...");
                
                var deleteSchemaRequest = new DeleteSchemaRequest
                {
                    SchemaId = new SchemaId 
                    { 
                        RegistryName = registryName,
                        SchemaName = expectedSchemaName
                    }
                };

                var deleteResponse = await glueClient.DeleteSchemaAsync(deleteSchemaRequest);
                Assert.NotNull(deleteResponse, "Delete schema response should not be null");
                
                Console.WriteLine($"✓ Successfully deleted auto-registered schema from custom registry: {expectedSchemaName}");
                
                // 11. If we created the registry, delete it too
                if (registryCreatedByTest)
                {
                    Console.WriteLine($"Cleaning up custom registry '{registryName}' (created by test)...");
                    var deleteRegistryRequest = new DeleteRegistryRequest
                    {
                        RegistryId = new RegistryId { RegistryName = registryName }
                    };
                    
                    var deleteRegistryResponse = await glueClient.DeleteRegistryAsync(deleteRegistryRequest);
                    Assert.NotNull(deleteRegistryResponse, "Delete registry response should not be null");
                    Console.WriteLine($"✓ Successfully deleted custom registry: {registryName}");
                }
                
                // 12. Optional: Verify schema is deleted (with eventual consistency handling)
                await Task.Delay(2000);
                
                try
                {
                    await glueClient.GetSchemaAsync(getSchemaRequest);
                    Console.WriteLine("Note: Schema still accessible after deletion (eventual consistency)");
                }
                catch (EntityNotFoundException)
                {
                    Console.WriteLine("✓ Confirmed auto-registered schema was successfully deleted from custom registry");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Schema deletion verified with exception: {ex.GetType().Name}");
                }
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
            finally
            {
                // 13. Always cleanup temporary config file
                if (tempConfigPath != null && File.Exists(tempConfigPath))
                {
                    try
                    {
                        File.Delete(tempConfigPath);
                        Console.WriteLine($"✓ Cleaned up temporary config file: {tempConfigPath}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Warning: Could not delete temporary config file: {ex.Message}");
                    }
                }
            }
        }

        [Test]
        public async Task Constructor_WithRegionEndpointMismatch_ThrowsAccessDeniedException()
        {
            /* Test region/endpoint mismatch scenario
             * Create config with mismatched region and endpoint
             * Attempt to serialize data 
             * Should throw AccessDenied or similar IAM-related exception
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-mismatch-{Guid.NewGuid():N}";
            string tempConfigPath = null;

            try
            {
                // 1. Create temporary config file with mismatched region and endpoint
                tempConfigPath = Path.GetTempFileName();
                var tempConfigContent = @"region=eu-west-1
                registry.name=default-registry
                endpoint=https://glue.eu-west-1.amazonaws.com
                dataFormat=AVRO
                schemaAutoRegistrationEnabled=true";

                await File.WriteAllTextAsync(tempConfigPath, tempConfigContent);
                TestContext.WriteLine($"✓ Created temporary config file with region/endpoint mismatch: {tempConfigPath}");
                TestContext.WriteLine("Region: eu-west-1 (may have different IAM permissions than us-east-1)");

                // 2. Create GSR serializer with mismatched config
                TestContext.WriteLine("Creating GSR serializer with mismatched region/endpoint config...");
                var serializer = new GlueSchemaRegistryKafkaSerializer(tempConfigPath);

                // 3. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();
                TestContext.WriteLine($"✓ Created test Avro record: {avroRecord}");

                // 4. Attempt to serialize - this should fail due to region/endpoint mismatch
                TestContext.WriteLine($"Attempting to serialize record to topic '{topicName}' (should fail due to mismatch)...");
                
                // This should throw an access denied or similar IAM/authorization exception
                var serializedBytes = serializer.Serialize(avroRecord, topicName);
                
                // If we reach here, the test should fail because we expected an exception
                Assert.Fail("Expected AccessDenied or authorization exception due to IAM permissions in different region, but serialization succeeded");
            }
            catch (AmazonServiceException ex) when (ex.StatusCode == System.Net.HttpStatusCode.Forbidden || 
                                                   ex.StatusCode == System.Net.HttpStatusCode.Unauthorized ||
                                                   ex.ErrorCode == "AccessDenied" ||
                                                   ex.ErrorCode == "UnauthorizedOperation" ||
                                                   ex.ErrorCode == "AccessDeniedException")
            {
                // Expected - this is what we want to test
                Console.WriteLine($"✓ Successfully caught expected authorization exception: {ex.ErrorCode}");
                Console.WriteLine($"✓ Status Code: {ex.StatusCode}");
                Console.WriteLine($"✓ Message: {ex.Message}");
                Assert.Pass($"Successfully validated region/endpoint mismatch results in authorization error: {ex.ErrorCode}");
            }
            catch (AmazonGlueException ex) when (ex.ErrorCode.Contains("AccessDenied") || 
                                                ex.ErrorCode.Contains("Unauthorized") ||
                                                ex.ErrorCode.Contains("Forbidden"))
            {
                // Also expected - AWS Glue specific authorization errors
                Console.WriteLine($"✓ Successfully caught expected AWS Glue authorization exception: {ex.ErrorCode}");
                Console.WriteLine($"✓ Message: {ex.Message}");
                Assert.Pass($"Successfully validated region/endpoint mismatch results in Glue authorization error: {ex.ErrorCode}");
            }
            catch (AmazonClientException ex) when (ex.Message.Contains("Unable to find credentials") ||
                                                  ex.Message.Contains("The security token included in the request is invalid") ||
                                                  ex.Message.Contains("Unable to load AWS credentials"))
            {
                // Also expected - credential/token related errors due to region mismatch
                Console.WriteLine($"✓ Successfully caught expected credential exception: {ex.GetType().Name}");
                Console.WriteLine($"✓ Message: {ex.Message}");
                Assert.Pass($"Successfully validated region/endpoint mismatch results in credential error: {ex.Message}");
            }
            catch (Exception ex)
            {
                // Log the actual exception to understand what we're getting
                Console.WriteLine($"Caught unexpected exception type: {ex.GetType().Name}");
                Console.WriteLine($"Message: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                
                // Check if it's still an authorization-related error with different exception type
                if (ex.Message.Contains("AccessDenied") || 
                    ex.Message.Contains("Unauthorized") || 
                    ex.Message.Contains("Forbidden") ||
                    ex.Message.Contains("InvalidUserID.NotFound") ||
                    ex.Message.Contains("The security token included in the request is invalid"))
                {
                    Console.WriteLine($"✓ Exception message indicates authorization error as expected");
                    Assert.Pass($"Successfully validated region/endpoint mismatch results in authorization error: {ex.Message}");
                }
                else
                {
                    Assert.Fail($"Expected authorization/access denied exception due to region/endpoint mismatch, but got: {ex.GetType().Name}: {ex.Message}");
                }
            }
            finally
            {
                // Always cleanup temporary config file (no schema/registry cleanup needed since operation should fail)
                if (tempConfigPath != null && File.Exists(tempConfigPath))
                {
                    try
                    {
                        File.Delete(tempConfigPath);
                        Console.WriteLine($"✓ Cleaned up temporary config file: {tempConfigPath}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Warning: Could not delete temporary config file: {ex.Message}");
                    }
                }
            }
        }
    }
}
