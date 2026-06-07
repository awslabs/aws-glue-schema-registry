using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.Glue;
using Amazon.Glue.Model;
using NUnit.Framework;
using AWSGsrSerDe.serializer;

namespace AWSGsrSerDe.Tests.utils
{
    /// <summary>
    /// Base class for integration tests that provides common functionality for AWS Glue Schema Registry testing.
    /// Handles registry creation/cleanup, schema management, and client lifecycle.
    /// </summary>
    public abstract class IntegrationTestBase
    {
        protected const string DEFAULT_REGISTRY_NAME = "default-registry";
        protected const string CUSTOM_REGISTRY_NAME = "native-test-registry";
        protected const string DEFAULT_REGION = "us-east-1";
        
        // Multi-region client manager - creates and caches clients per region
        private readonly Dictionary<string, IAmazonGlue> _regionClients = new();
        private readonly List<(string registryName, string schemaName, string region)> _schemasToCleanup = new();
        private readonly List<(string registryName, string region)> _registriesToCleanup = new();

        /// <summary>
        /// Gets or creates a Glue client for the specified region.
        /// Caches clients to avoid recreating them for the same region.
        /// </summary>
        protected IAmazonGlue GetGlueClientForRegion(string region = DEFAULT_REGION)
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
        protected IAmazonGlue GetGlueClientForConfig(string configPath)
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
                if (!System.IO.File.Exists(configPath))
                {
                    return DEFAULT_REGION;
                }

                var configLines = System.IO.File.ReadAllLines(configPath);
                var regionLine = Array.Find(configLines, line => line.StartsWith("region="));
                
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

        /// <summary>
        /// Ensures that a registry exists in the specified region, creating it if necessary.
        /// </summary>
        /// <param name="registryName">Name of the registry to ensure exists</param>
        /// <param name="region">AWS region where the registry should exist</param>
        /// <param name="description">Description for the registry if it needs to be created</param>
        protected async Task EnsureRegistryExists(string registryName, string region = DEFAULT_REGION, string description = null)
        {
            var glueClient = GetGlueClientForRegion(region);
            description ??= $"Test registry '{registryName}' in region '{region}' created by integration test";
            
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
                    Description = description
                };

                var createRegistryResponse = await glueClient.CreateRegistryAsync(createRegistryRequest);
                Assert.NotNull(createRegistryResponse, "Create registry response should not be null");
                Assert.NotNull(createRegistryResponse.RegistryArn, "Registry ARN should not be null");

                // Mark for cleanup since the test-suite created it
                MarkRegistryForCleanup(registryName, region);
            }
        }

        /// <summary>
        /// Waits for a schema to be registered in the specified registry and region.
        /// </summary>
        protected async Task<bool> WaitForSchemaRegistration(string registryName, string schemaName, string region = DEFAULT_REGION, int maxRetries = 10, int delayMs = 500)
        {
            var glueClient = GetGlueClientForRegion(region);
            
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

        /// <summary>
        /// Creates a GlueSchemaRegistryKafkaSerializer instance using the specified shared config file.
        /// </summary>
        protected GlueSchemaRegistryKafkaSerializer CreateSerializer(string configFileName)
        {
            var configPath = GetSharedConfigPath(configFileName);
            return new GlueSchemaRegistryKafkaSerializer(configPath);
        }

        /// <summary>
        /// Marks a schema for cleanup during teardown.
        /// </summary>
        protected void MarkSchemaForCleanup(string registryName, string schemaName, string region = DEFAULT_REGION)
        {
            _schemasToCleanup.Add((registryName, schemaName, region));
        }

        /// <summary>
        /// Marks a registry for cleanup during teardown.
        /// </summary>
        protected void MarkRegistryForCleanup(string registryName, string region = DEFAULT_REGION)
        {
            _registriesToCleanup.Add((registryName, region));
        }

        /// <summary>
        /// Cleans up all schemas and registries created during tests.
        /// Called automatically as [TearDown] method.
        /// </summary>
        [TearDown]
        public virtual async Task CleanupResources()
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

        /// <summary>
        /// Gets the full path to a shared test configuration file.
        /// </summary>
        /// <param name="configFileName">The name of the config file (e.g., "minimal-auto-registration-default-registry.properties")</param>
        /// <returns>The full path to the configuration file</returns>
        protected string GetSharedConfigPath(string configFileName)
        {
            var assemblyDir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
            var configPath = System.IO.Path.Combine(assemblyDir, "../../../../../../shared/test/configs", configFileName);
            return System.IO.Path.GetFullPath(configPath);
        }

        /// <summary>
        /// Loads a shared Avro schema file and returns its content.
        /// </summary>
        /// <param name="schemaFileName">The relative path to the schema file under shared/test/avro/</param>
        /// <returns>The schema file content as string</returns>
        protected string LoadSharedAvroSchema(string schemaFileName)
        {
            var assemblyDir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)!;
            var schemaPath = System.IO.Path.Combine(assemblyDir, "../../../../../../shared/test/avro", schemaFileName);
            var fullPath = System.IO.Path.GetFullPath(schemaPath);
            return System.IO.File.ReadAllText(fullPath);
        }
    }
}
