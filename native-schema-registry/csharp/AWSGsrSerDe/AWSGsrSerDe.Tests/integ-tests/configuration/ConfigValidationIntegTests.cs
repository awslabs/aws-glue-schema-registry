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
            var registryName = "default-registry";
            string tempConfigPath = null;

            try
            {
                // 1. Create temporary config file with auto-registration enabled and no registry specification
                tempConfigPath = Path.GetTempFileName();
                var tempConfigContent = $@"region=us-east-1
                dataFormat=AVRO
                schemaAutoRegistrationEnabled=true";

                await File.WriteAllTextAsync(tempConfigPath, tempConfigContent);

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

                // 6. Wait for eventual API calls to complete
                Console.WriteLine("Waiting for API calls to complete");
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

                var getResponse = await glueClient.GetSchemaAsync(getSchemaRequest);

                Assert.NotNull(getResponse, "Get schema response should not be null");
                Assert.That(getResponse.SchemaName, Is.EqualTo(expectedSchemaName), "Auto-registered schema name should match topic name");
                Assert.That(getResponse.DataFormat, Is.EqualTo(DataFormat.AVRO), "Auto-registered schema should be AVRO format");

                // 9. Cleanup - Delete the auto-registered schema
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
                // 11. Cleanup temporary config file
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
                try
                {
                    var getRegistryRequest = new GetRegistryRequest
                    {
                        RegistryId = new RegistryId { RegistryName = registryName }
                    };

                    await glueClient.GetRegistryAsync(getRegistryRequest);
                }
                catch (EntityNotFoundException)
                {
                    // Registry doesn't exist, create it
                    var createRegistryRequest = new CreateRegistryRequest
                    {
                        RegistryName = registryName,
                        Description = "Test registry created by integration test"
                    };

                    var createRegistryResponse = await glueClient.CreateRegistryAsync(createRegistryRequest);
                    Assert.NotNull(createRegistryResponse, "Create registry response should not be null");
                    Assert.NotNull(createRegistryResponse.RegistryArn, "Registry ARN should not be null");

                    // To cleanup only if created by this test
                    registryCreatedByTest = true;
                }

                // 3. Create temporary config file with auto-registration enabled for custom registry (native-test-registry)
                tempConfigPath = Path.GetTempFileName();
                var tempConfigContent = $@"region=us-east-1
                    registry.name={registryName}
                    dataFormat=AVRO
                    schemaAutoRegistrationEnabled=true";

                await File.WriteAllTextAsync(tempConfigPath, tempConfigContent);

                // 4. Create GSR serializer with auto-registration enabled for custom registry
                var serializer = new GlueSchemaRegistryKafkaSerializer(tempConfigPath);

                // 5. Create Avro record to serialize
                var avroRecord = RecordGenerator.GetTestAvroRecord();

                // 6. Serialize the record - this should auto-register the schema in custom registry
                var serializedBytes = serializer.Serialize(avroRecord, topicName);

                Assert.NotNull(serializedBytes, "Serialized bytes should not be null");
                Assert.That(serializedBytes.Length, Is.GreaterThan(0), "Serialized bytes should not be empty");

                // 7. Wait for API calls to complete
                Console.WriteLine("Waiting for API calls to complete");
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

                var getResponse = await glueClient.GetSchemaAsync(getSchemaRequest);

                Assert.NotNull(getResponse, "Get schema response should not be null");
                Assert.That(getResponse.SchemaName, Is.EqualTo(expectedSchemaName), "Auto-registered schema name should match topic name");
                Assert.That(getResponse.DataFormat, Is.EqualTo(DataFormat.AVRO), "Auto-registered schema should be AVRO format");
                Assert.That(getResponse.RegistryName, Is.EqualTo(registryName), "Schema should be in the custom registry");

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
             * Should throw AccessDenied exception as the IAM permissions for the configured region will not apply to the endpoint's region
             * No cleanup needed as no resources should be created
             */

            // Generate unique topic name
            var topicName = $"test-topic-mismatch-{Guid.NewGuid():N}";
            string tempConfigPath = null;

            try
            {
                // 1. Create temporary config file with mismatched region and endpoint
                tempConfigPath = Path.GetTempFileName();
                var tempConfigContent = @"region=eu-west-2
                registry.name=default-registry
                endpoint=https://glue.eu-west-1.amazonaws.com
                dataFormat=AVRO
                schemaAutoRegistrationEnabled=true";

                await File.WriteAllTextAsync(tempConfigPath, tempConfigContent);

                // 2. Create GSR serializer with mismatched config
                TestContext.WriteLine("Creating GSR serializer with mismatched region/endpoint config...");
                var serializer = new GlueSchemaRegistryKafkaSerializer(tempConfigPath);

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
            string tempConfigPath = null;

            try
            {
                // 1. Create temporary config file with invalid region
                tempConfigPath = Path.GetTempFileName();
                var tempConfigContent = @"region=us-east-99
                registry.name=default-registry
                dataFormat=AVRO
                schemaAutoRegistrationEnabled=true";

                await File.WriteAllTextAsync(tempConfigPath, tempConfigContent);

                // 2. Create GSR serializer with invalid region config
                var serializer = new GlueSchemaRegistryKafkaSerializer(tempConfigPath);

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
            finally
            {
                // Always cleanup temporary config file (no schema/registry cleanup needed since operation should fail)
                if (tempConfigPath != null && File.Exists(tempConfigPath))
                {
                    try
                    {
                        File.Delete(tempConfigPath);
                    }
                    catch (Exception ex)
                    {
                        TestContext.WriteLine($"Warning: Could not delete temporary config file: {ex.Message}");
                    }
                }
            }
        }
    }
}
