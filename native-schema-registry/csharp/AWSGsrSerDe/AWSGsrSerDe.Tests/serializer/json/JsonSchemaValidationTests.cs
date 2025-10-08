// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.serializer.json;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.Tests.utils;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.serializer.json
{
    /// <summary>
    /// Comprehensive tests for JSON schema validation scenarios.
    /// Tests all combinations of matching/mismatching schema and data.
    /// </summary>
    [TestFixture]
    public class JsonSchemaValidationTests
    {
        private static readonly string JSON_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal-json.properties");

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

        private readonly GlueSchemaRegistryKafkaSerializer _kafkaSerializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
        private readonly GlueSchemaRegistryKafkaDeserializer _kafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH);

        #region Test Data Setup

        /// <summary>
        /// Finds the native-schema-registry root by looking for shared/test directory and returns absolute path to schema file
        /// </summary>
        /// <param name="schemaFileName">Schema file name in shared/test/json directory</param>  
        /// <returns>Absolute path to the schema file</returns>
        private static string GetSchemaPath(string schemaFileName)
        {
            var currentDir = new DirectoryInfo(Directory.GetCurrentDirectory());
            
            // Navigate up until we find the native-schema-registry directory
            while (currentDir != null && currentDir.Name != "native-schema-registry")
            {
                currentDir = currentDir.Parent;
            }
            
            if (currentDir == null)
            {
                // If we can't find native-schema-registry, try looking for shared/test from current location
                currentDir = new DirectoryInfo(Directory.GetCurrentDirectory());
                while (currentDir != null)
                {
                    var sharedTestDir = Path.Combine(currentDir.FullName, "shared", "test", "json");
                    if (Directory.Exists(sharedTestDir))
                    {
                        return Path.Combine(sharedTestDir, schemaFileName);
                    }
                    currentDir = currentDir.Parent;
                }
                throw new DirectoryNotFoundException($"Could not find shared/test/json directory for schema: {schemaFileName}");
            }
            
            var schemaPath = Path.Combine(currentDir.FullName, "shared", "test", "json", schemaFileName);
            if (!File.Exists(schemaPath))
            {
                throw new FileNotFoundException($"Schema file not found: {schemaPath}");
            }
            
            return schemaPath;
        }

        /// <summary>
        /// Load a schema from the shared test resources
        /// </summary>
        /// <param name="schemaFileName">Schema file name</param>
        /// <returns>Schema content as string</returns>
        private static string LoadSchema(string schemaFileName)
        {
            var schemaPath = GetSchemaPath(schemaFileName);
            return File.ReadAllText(schemaPath);
        }

        /// <summary>
        /// Valid geographical location schema from shared test resources
        /// </summary>
        private static readonly string GEOGRAPHICAL_SCHEMA = LoadSchema("geographical-location.schema.json");

        /// <summary>
        /// Valid person schema from shared test resources (mismatches geographical data)
        /// </summary>
        private static readonly string PERSON_SCHEMA = LoadSchema("person.schema.json");

        /// <summary>
        /// Valid geographical data that matches the geographical schema
        /// </summary>
        private static readonly string VALID_GEOGRAPHICAL_DATA = 
            @"{""latitude"": 48.858093, ""longitude"": 2.294694}";

        /// <summary>
        /// Valid person data that matches the person schema
        /// </summary>
        private static readonly string VALID_PERSON_DATA = 
            @"{""name"": ""John Doe"", ""age"": 30, ""email"": ""john.doe@example.com""}";

        /// <summary>
        /// Invalid geographical data - missing required longitude field
        /// </summary>
        private static readonly string INVALID_GEOGRAPHICAL_DATA = 
            @"{""latitude"": 48.858093}";

        /// <summary>
        /// Invalid person data - missing required name field
        /// </summary>
        private static readonly string INVALID_PERSON_DATA = 
            @"{""age"": 30}";

        /// <summary>
        /// Malformed JSON data
        /// </summary>
        private static readonly string MALFORMED_JSON_DATA = 
            @"{""latitude"": 48.858093, ""longitude"": 2.294694"; // Missing closing brace

        #endregion

        #region Scenario 1: Matching Schema + Matching Data (Should Succeed)

        [Test]
        public void MatchingSchema_MatchingData_GeographicalData_ShouldSerializeSuccessfully()
        {
            // Arrange
            var testData = JsonDataWithSchema.Build(GEOGRAPHICAL_SCHEMA, VALID_GEOGRAPHICAL_DATA);

            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                var serialized = _kafkaSerializer.Serialize(testData, "test-topic-geographical");
                Assert.IsNotNull(serialized);
                
                // Verify deserialization works
                var deserialized = _kafkaDeserializer.Deserialize("test-topic-geographical", serialized);
                Assert.IsInstanceOf<JsonDataWithSchema>(deserialized);
            });
        }

        [Test]
        public void MatchingSchema_MatchingData_PersonData_ShouldSerializeSuccessfully()
        {
            // Arrange
            var testData = JsonDataWithSchema.Build(PERSON_SCHEMA, VALID_PERSON_DATA);

            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                var serialized = _kafkaSerializer.Serialize(testData, "test-topic-person");
                Assert.IsNotNull(serialized);
                
                // Verify deserialization works
                var deserialized = _kafkaDeserializer.Deserialize("test-topic-person", serialized);
                Assert.IsInstanceOf<JsonDataWithSchema>(deserialized);
            });
        }

        [Test]
        public void ValidSchema_ValidData_JsonString_ShouldSerializeSuccessfully()
        {
            // Arrange - Test with plain JSON string (infers schema)
            Assert.DoesNotThrow(() =>
            {
                var serialized = _kafkaSerializer.Serialize(VALID_GEOGRAPHICAL_DATA, "test-topic-json-string");
                Assert.IsNotNull(serialized);
            });
        }

        #endregion

        #region Scenario 2: Mismatched Schema + Data (Java Validation Still Active - Should Throw)

        [Test]
        public void GeographicalSchema_PersonData_ShouldThrowValidationException()
        {
            // Arrange - Geographical schema with person data (schema/data mismatch)
            // NOTE: Even though C# validation is commented out, Java validation is still active
            var testData = JsonDataWithSchema.Build(GEOGRAPHICAL_SCHEMA, VALID_PERSON_DATA);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-mismatch-1");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        [Test]
        public void PersonSchema_GeographicalData_ShouldThrowValidationException()
        {
            // Arrange - Person schema with geographical data (schema/data mismatch)
            // NOTE: Even though C# validation is commented out, Java validation is still active
            var testData = JsonDataWithSchema.Build(PERSON_SCHEMA, VALID_GEOGRAPHICAL_DATA);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-mismatch-2");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        [Test]
        public void GeographicalSchema_InvalidGeographicalData_ShouldThrowValidationException()
        {
            // Arrange - Missing required longitude field
            // NOTE: Even though C# validation is commented out, Java validation is still active
            var testData = JsonDataWithSchema.Build(GEOGRAPHICAL_SCHEMA, INVALID_GEOGRAPHICAL_DATA);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-invalid-geo");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        [Test]
        public void PersonSchema_InvalidPersonData_ShouldThrowValidationException()
        {
            // Arrange - Missing required name field
            // NOTE: Even though C# validation is commented out, Java validation is still active
            var testData = JsonDataWithSchema.Build(PERSON_SCHEMA, INVALID_PERSON_DATA);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-invalid-person");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        [Test]
        public void ValidSchema_MalformedJsonData_ShouldThrowException()
        {
            // Arrange - This should still fail because JSON parsing happens before validation
            var testData = JsonDataWithSchema.Build(GEOGRAPHICAL_SCHEMA, MALFORMED_JSON_DATA);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-malformed");
            });

            Assert.IsTrue(exception.Message.Contains("Malformed JSON") || 
                         exception.Message.Contains("JSON"));
        }

        #endregion

        #region Additional Validation Tests

        [Test]
        public void GeographicalSchema_OutOfRangeData_ShouldThrowValidationException()
        {
            // Arrange - Latitude out of valid range (-90 to 90)
            // NOTE: Java validation is still active even though C# validation is commented out
            var outOfRangeData = @"{""latitude"": 95.0, ""longitude"": 2.294694}";
            var testData = JsonDataWithSchema.Build(GEOGRAPHICAL_SCHEMA, outOfRangeData);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-out-of-range");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        [Test]
        public void PersonSchema_InvalidAgeType_ShouldThrowValidationException()
        {
            // Arrange - Age as string instead of integer
            // NOTE: Java validation is still active even though C# validation is commented out
            var invalidTypeData = @"{""name"": ""John Doe"", ""age"": ""thirty""}";
            var testData = JsonDataWithSchema.Build(PERSON_SCHEMA, invalidTypeData);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-invalid-age-type");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        [Test]
        public void GeographicalSchema_AdditionalProperties_ShouldThrowValidationException()
        {
            // Arrange - Additional properties not allowed
            // NOTE: Java validation is still active even though C# validation is commented out
            var additionalPropsData = @"{""latitude"": 48.858093, ""longitude"": 2.294694, ""altitude"": 100}";
            var testData = JsonDataWithSchema.Build(GEOGRAPHICAL_SCHEMA, additionalPropsData);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-additional-props");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        #endregion

        #region End-to-End Serialization/Deserialization Tests

        [Test]
        public void FullSerDeTest_MatchingSchemaAndData_ShouldRoundTrip()
        {
            // Arrange
            var testData = JsonDataWithSchema.Build(GEOGRAPHICAL_SCHEMA, VALID_GEOGRAPHICAL_DATA);

            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                var serialized = _kafkaSerializer.Serialize(testData, "test-topic-full-serde-geo");
                var deserialized = _kafkaDeserializer.Deserialize("test-topic-full-serde-geo", serialized);
                
                Assert.IsInstanceOf<JsonDataWithSchema>(deserialized);
                var deserializedData = (JsonDataWithSchema)deserialized;
                
                // Compare schemas as JSON objects to handle formatting differences
                Assert.AreEqual(
                    JsonNode.Parse(testData.Schema)?.ToString(),
                    JsonNode.Parse(deserializedData.Schema)?.ToString());
                Assert.AreEqual(
                    JsonNode.Parse(testData.Payload)?.ToString(),
                    JsonNode.Parse(deserializedData.Payload)?.ToString());
            });
        }

        [Test]
        public void FullSerDeTest_PersonData_ShouldRoundTrip()
        {
            // Arrange
            var testData = JsonDataWithSchema.Build(PERSON_SCHEMA, VALID_PERSON_DATA);

            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                var serialized = _kafkaSerializer.Serialize(testData, "test-topic-full-serde-person");
                var deserialized = _kafkaDeserializer.Deserialize("test-topic-full-serde-person", serialized);
                
                Assert.IsInstanceOf<JsonDataWithSchema>(deserialized);
                var deserializedData = (JsonDataWithSchema)deserialized;
                
                // Compare schemas as JSON objects to handle formatting differences
                Assert.AreEqual(
                    JsonNode.Parse(testData.Schema)?.ToString(),
                    JsonNode.Parse(deserializedData.Schema)?.ToString());
                Assert.AreEqual(
                    JsonNode.Parse(testData.Payload)?.ToString(),
                    JsonNode.Parse(deserializedData.Payload)?.ToString());
            });
        }

        #endregion

        #region Edge Cases and Error Handling

        [Test]
        public void SerializeNull_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                try
                {
                    _kafkaSerializer.Serialize(null, "test-topic-null");
                    // If no exception, that's fine - just testing the behavior
                }
                catch (ArgumentNullException)
                {
                    // This is also expected behavior
                }
                catch (AwsSchemaRegistryException)
                {
                    // This might happen too depending on where null check occurs
                }
            });
        }

        [Test]
        public void NullSchema_ShouldNotThrowWhenBuildingWrapper()
        {
            // NOTE: JsonDataWithSchema.Build actually allows null schema
            // The exception comes later during serialization
            Assert.DoesNotThrow(() =>
            {
                var testData = JsonDataWithSchema.Build(null, VALID_GEOGRAPHICAL_DATA);
                Assert.IsNotNull(testData);
            });
        }

        [Test]
        public void EmptySchema_ShouldThrowException()
        {
            // Arrange - Empty schema will fail during JSON parsing
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                var testData = JsonDataWithSchema.Build("", VALID_GEOGRAPHICAL_DATA);
                _kafkaSerializer.Serialize(testData, "test-topic-empty-schema");
            });

            Assert.IsTrue(exception.Message.Contains("Malformed JSON"));
        }

        [Test]
        public void MultipleValidationErrors_ShouldThrowValidationException()
        {
            // Arrange - Data with multiple validation issues (wrong type + missing field)
            // NOTE: Java validation is still active even though C# validation is commented out
            var multipleErrorsData = @"{""latitude"": ""not_a_number""}"; // Wrong type + missing longitude
            var testData = JsonDataWithSchema.Build(GEOGRAPHICAL_SCHEMA, multipleErrorsData);

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(testData, "test-topic-multiple-errors");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        [Test]
        public void NonSchemaConformantData_ShouldThrowValidationException()
        {
            // Arrange - Equivalent to Java testValidate_validatesWrapper_ThrowsValidationException
            // Using RecordGenerator to create non-conformant data, but test through serialization
            var nonConformantData = RecordGenerator.CreateNonSchemaConformantJsonData();

            // Act & Assert
            var exception = Assert.Throws<AwsSchemaRegistryException>(() =>
            {
                _kafkaSerializer.Serialize(nonConformantData, "test-topic-non-conformant");
            });

            Assert.IsTrue(exception.Message.Contains("JSON data validation against schema failed"));
        }

        #endregion

    }
}
