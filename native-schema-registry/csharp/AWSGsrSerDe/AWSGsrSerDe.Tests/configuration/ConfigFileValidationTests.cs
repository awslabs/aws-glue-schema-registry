using System;
using System.IO;
using System.Linq;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.Configuration
{
    [TestFixture]
    public class ConfigFileValidationTests
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
        public void Constructor_WithValidMinimalConfig_CreatesSerializerSuccessfully()
        {
            // Arrange
            var configPath = GetConfigPath("configuration/test-configs/valid-minimal.properties");

            // Act & Assert
            using var serializer = new GlueSchemaRegistrySerializer(configPath);
            Assert.IsNotNull(serializer);
        }

        [Test]
        public void Constructor_WithValidMaximalConfig_CreatesSerializerSuccessfully()
        {
            // Arrange  
            var configPath = GetConfigPath("configuration/test-configs/valid-maximal.properties");

            // Act & Assert
            using var serializer = new GlueSchemaRegistrySerializer(configPath);
            Assert.IsNotNull(serializer);
        }

        [Test]
        public void Constructor_WithValidMinimalConfig_CreatesDeserializerSuccessfully()
        {
            // Arrange
            var configPath = GetConfigPath("configuration/test-configs/valid-minimal.properties");

            // Act & Assert
            using var deserializer = new GlueSchemaRegistryDeserializer(configPath);
            Assert.IsNotNull(deserializer);
        }

        [Test]
        public void Constructor_WithValidMaximalConfig_CreatesDeserializerSuccessfully()
        {
            // Arrange
            var configPath = GetConfigPath("configuration/test-configs/valid-maximal.properties");

            // Act & Assert
            using var deserializer = new GlueSchemaRegistryDeserializer(configPath);
            Assert.IsNotNull(deserializer);
        }

        [Test]
        public void Constructor_WithNullConfigPath_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => new GlueSchemaRegistrySerializer(null));
            Assert.Throws<ArgumentException>(() => new GlueSchemaRegistryDeserializer(null));
        }

        [Test]
        public void Constructor_WithEmptyConfigPath_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => new GlueSchemaRegistrySerializer(""));
            Assert.Throws<ArgumentException>(() => new GlueSchemaRegistryDeserializer(""));
        }

        [Test]
        public void Constructor_WithNonExistentConfigFile_ThrowsException()
        {
            // Arrange
            var nonExistentPath = GetConfigPath("configuration/test-configs/nonexistent.properties");

            // Act & Assert
            Assert.Throws<FileNotFoundException>(() => new GlueSchemaRegistrySerializer(nonExistentPath));
            Assert.Throws<FileNotFoundException>(() => new GlueSchemaRegistryDeserializer(nonExistentPath));
        }


        [Test]
        public void Constructor_WithCustomConfig_AcceptsAllProperties()
        {
            // Arrange - Create a dynamic config file with various property combinations
            var configPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, "custom-test-config.properties");
            var configContent = @"region=eu-west-1
                registry.name=custom-registry
                dataFormat=JSON
                schemaAutoRegistrationEnabled=false
                timeToLiveMillis=120000
                cacheSize=500
                compression=ZLIB
                compatibility=FORWARD
                description=Custom test configuration
                userAgentApp=CustomTestApp
                ";

            File.WriteAllText(configPath, configContent);

            try
            {
                // Act & Assert
                using var serializer = new GlueSchemaRegistrySerializer(configPath);
                using var deserializer = new GlueSchemaRegistryDeserializer(configPath);
                
                Assert.IsNotNull(serializer);
                Assert.IsNotNull(deserializer);
            }
            finally
            {
                // Cleanup
                if (File.Exists(configPath))
                    File.Delete(configPath);
            }
        }

        [Test]
        public void Constructor_WithDifferentRegions_CreatesInstancesSuccessfully()
        {
            // Test various AWS regions
            var regions = new[] { "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1" };

            foreach (var region in regions)
            {
                // Arrange
                var configPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, $"region-{region}-config.properties");
                var configContent = $@"region={region}
                    registry.name=test-registry-{region}
                    dataFormat=AVRO
                    schemaAutoRegistrationEnabled=true
                    ";

                File.WriteAllText(configPath, configContent);

                try
                {
                    // Act & Assert
                    using var serializer = new GlueSchemaRegistrySerializer(configPath);
                    using var deserializer = new GlueSchemaRegistryDeserializer(configPath);
                    
                    Assert.IsNotNull(serializer, $"Serializer should be created for region {region}");
                    Assert.IsNotNull(deserializer, $"Deserializer should be created for region {region}");
                }
                finally
                {
                    // Cleanup
                    if (File.Exists(configPath))
                        File.Delete(configPath);
                }
            }
        }

        [Test]
        public void Constructor_WithDifferentDataFormats_CreatesInstancesSuccessfully()
        {
            // Test various data formats
            var dataFormats = new[] { "AVRO", "JSON", "PROTOBUF" };

            foreach (var dataFormat in dataFormats)
            {
                // Arrange
                var configPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, $"format-{dataFormat}-config.properties");
                var configContent = $@"region=us-east-1
registry.name=test-registry
dataFormat={dataFormat}
schemaAutoRegistrationEnabled=true
";

                File.WriteAllText(configPath, configContent);

                try
                {
                    // Act & Assert
                    using var serializer = new GlueSchemaRegistrySerializer(configPath);
                    using var deserializer = new GlueSchemaRegistryDeserializer(configPath);
                    
                    Assert.IsNotNull(serializer, $"Serializer should be created for data format {dataFormat}");
                    Assert.IsNotNull(deserializer, $"Deserializer should be created for data format {dataFormat}");
                }
                finally
                {
                    // Cleanup
                    if (File.Exists(configPath))
                        File.Delete(configPath);
                }
            }
        }

        [Test]
        public void Constructor_WithDifferentCompatibilitySettings_CreatesInstancesSuccessfully()
        {
            // Test various compatibility settings
            var compatibilitySettings = new[] { "NONE", "DISABLED", "BACKWARD", "BACKWARD_ALL", "FORWARD", "FORWARD_ALL", "FULL", "FULL_ALL" };

            foreach (var compatibility in compatibilitySettings)
            {
                // Arrange
                var configPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, $"compat-{compatibility}-config.properties");
                var configContent = $@"region=us-east-1
                    registry.name=test-registry
                    dataFormat=AVRO
                    schemaAutoRegistrationEnabled=true
                    compatibility={compatibility}
                    ";

                File.WriteAllText(configPath, configContent);

                try
                {
                    // Act & Assert
                    using var serializer = new GlueSchemaRegistrySerializer(configPath);
                    using var deserializer = new GlueSchemaRegistryDeserializer(configPath);
                    
                    Assert.IsNotNull(serializer, $"Serializer should be created for compatibility {compatibility}");
                    Assert.IsNotNull(deserializer, $"Deserializer should be created for compatibility {compatibility}");
                }
                finally
                {
                    // Cleanup
                    if (File.Exists(configPath))
                        File.Delete(configPath);
                }
            }
        }

        [Test]
        public void Constructor_WithBooleanValues_HandlesCorrectly()
        {
            // Test boolean property handling
            var booleanValues = new[] { "true", "false", "TRUE", "FALSE", "True", "False" };

            foreach (var boolValue in booleanValues)
            {
                // Arrange
                var configPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, $"bool-{boolValue}-config.properties");
                var configContent = $@"region=us-east-1
                    registry.name=test-registry
                    dataFormat=AVRO
                    schemaAutoRegistrationEnabled={boolValue}
                    ";

                File.WriteAllText(configPath, configContent);

                try
                {
                    // Act & Assert
                    using var serializer = new GlueSchemaRegistrySerializer(configPath);
                    Assert.IsNotNull(serializer, $"Serializer should handle boolean value {boolValue}");
                }
                finally
                {
                    // Cleanup
                    if (File.Exists(configPath))
                        File.Delete(configPath);
                }
            }
        }

        [Test]
        public void Constructor_WithNumericValues_HandlesCorrectly()
        {
            // Test numeric property handling
            var numericTestCases = new[]
            {
                ("300000", "1000"),      // Normal values
                ("0", "0"),              // Zero values
                ("1", "1"),              // Minimum values
                ("3600000", "10000")     // Large values
            };

            foreach (var (ttl, cacheSize) in numericTestCases)
            {
                // Arrange
                var configPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, $"numeric-{ttl}-{cacheSize}-config.properties");
                var configContent = $@"region=us-east-1
                    registry.name=test-registry
                    dataFormat=AVRO
                    schemaAutoRegistrationEnabled=true
                    timeToLiveMillis={ttl}
                    cacheSize={cacheSize}
                    ";

                File.WriteAllText(configPath, configContent);

                try
                {
                    // Act & Assert
                    using var serializer = new GlueSchemaRegistrySerializer(configPath);
                    Assert.IsNotNull(serializer, $"Serializer should handle TTL={ttl}, CacheSize={cacheSize}");
                }
                finally
                {
                    // Cleanup
                    if (File.Exists(configPath))
                        File.Delete(configPath);
                }
            }
        }
    }
}
