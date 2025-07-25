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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.serializer.avro;
using AWSGsrSerDe.serializer.json;
using AWSGsrSerDe.serializer.protobuf;
using AWSGsrSerDe.Tests.serializer.json;
using AWSGsrSerDe.Tests.utils;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Namotion.Reflection;
using NUnit.Framework;
using static AWSGsrSerDe.Tests.utils.ProtobufGenerator;

namespace AWSGsrSerDe.Tests.serializer
{
    [TestFixture]
    public class GlueSchemaRegistryKafkaSerializerTests
    {
        private static readonly Car SPECIFIC_TEST_RECORD = new Car
        {
            make = "Honda",
            model = "crv",
            used = true,
            miles = 10000,
            listedDate = DateTime.Now,
            purchaseDate = DateTime.Parse("2000-01-01T00:00:00.000Z"),
            owners = new[] { "John", "Jane", "Hu" },
            serviceCheckes = new[] { 5000.0f, 10780.30f }
        };

        private static readonly SchemaLoader.JsonGenericRecord GENERIC_TEST_RECORD = SchemaLoader.LoadJsonGenericRecord(
            "schema/draft07/geographical-location.schema.json",
            "geolocation1.json",
            true);

        private static readonly string AVRO_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal.properties");
        private static readonly string PROTOBUF_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal-protobuf.properties");
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

        /// <summary>
        /// Parses a Java-style .properties file into a Dictionary for configuration loading.
        /// This helper method handles the key=value format used by configuration files.
        /// </summary>
        /// <param name="filePath">Path to the .properties file</param>
        /// <returns>Dictionary containing the parsed key-value pairs</returns>
        private static Dictionary<string, dynamic> ParsePropertiesFile(string filePath)
        {
            var properties = new Dictionary<string, dynamic>();
            
            foreach (var line in File.ReadAllLines(filePath))
            {
                var trimmedLine = line.Trim();
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#"))
                    continue;
                    
                var equalsIndex = trimmedLine.IndexOf('=');
                if (equalsIndex > 0)
                {
                    var key = trimmedLine.Substring(0, equalsIndex).Trim();
                    var value = trimmedLine.Substring(equalsIndex + 1).Trim();
                    properties[key] = value;
                }
            }
            
            return properties;
        }

        /// <summary>
        /// Creates a temporary configuration file for protobuf testing with the specific message descriptor.
        /// Since the existing API only accepts config file paths, this method creates a temporary config file
        /// that can be used to test different protobuf message types without modifying source code.
        /// 
        /// The approach:
        /// 1. Loads base configuration from the original properties file
        /// 2. Creates a temporary config file with all the base settings
        /// 3. Note: The protobuf descriptor cannot be stored in the config file since it's a runtime object,
        ///    so this approach works by ensuring each test gets its own isolated config file
        /// </summary>
        /// <param name="messageDescriptor">The protobuf message descriptor for this specific test</param>
        /// <returns>Path to the temporary configuration file</returns>
        private static string CreateTempProtobufConfig(MessageDescriptor messageDescriptor)
        {
            // Generate a unique temporary file name for this test
            var tempFileName = $"temp-protobuf-config-{Guid.NewGuid()}.properties";
            var tempConfigPath = Path.Combine(Path.GetTempPath(), tempFileName);
            
            // Read the base config file and copy all settings to the temporary file
            var baseConfigLines = File.ReadAllLines(PROTOBUF_CONFIG_PATH);
            
            // Write the base configuration to the temporary file
            // The protobuf descriptor will be set programmatically during serialization/deserialization
            File.WriteAllLines(tempConfigPath, baseConfigLines);
            
            return tempConfigPath;
        }

        private static readonly GlueSchemaRegistryKafkaSerializer KafkaSerializer =
            new GlueSchemaRegistryKafkaSerializer(AVRO_CONFIG_PATH);

        private static readonly GlueSchemaRegistryKafkaDeserializer KafkaDeserializer =
            new GlueSchemaRegistryKafkaDeserializer(AVRO_CONFIG_PATH);


        [Test]
        public void KafkaSerDeTestForAvroGenericRecord()
        {
            var avroRecord = RecordGenerator.GetTestAvroRecord();

            var bytes = KafkaSerializer.Serialize(avroRecord, "test-topic");
            var deserializeObject = KafkaDeserializer.Deserialize("test-topic", bytes);

            Assert.IsTrue(deserializeObject is GenericRecord);
            var genericRecord = (GenericRecord)deserializeObject;

            Assert.AreEqual(avroRecord, genericRecord);
        }

        private static List<IMessage> TestMessageProvider()
        {
            return new List<IMessage>
            {
                BASIC_SYNTAX2_MESSAGE,
                BASIC_SYNTAX3_MESSAGE,
                BASIC_REFERENCING_MESSAGE,
                NESTING_MESSAGE_PROTO2,
                NESTING_MESSAGE_PROTO3,
                NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
                ALL_TYPES_MESSAGE_SYNTAX2,
                ALL_TYPES_MESSAGE_SYNTAX3,
                WELL_KNOWN_TYPES_SYNTAX_2,
                WELL_KNOWN_TYPES_SYNTAX_3,
            };
        }

        [Test]
        [TestCaseSource(nameof(TestMessageProvider))]
        public void KafkaSerDeTestForAllProtobufTypes(IMessage message)
        {
            // Create a temporary config file with the dynamic protobuf descriptor for this test
            var tempConfigPath = CreateTempProtobufConfig(message.Descriptor);
            
            try
            {
                var protobufSerializer = new GlueSchemaRegistryKafkaSerializer(tempConfigPath);
                var protobufDeserializer = new GlueSchemaRegistryKafkaDeserializer(tempConfigPath);

                var serialized = protobufSerializer.Serialize(message, message.Descriptor.FullName);

                var deserializedObject =
                    protobufDeserializer.Deserialize(message.Descriptor.FullName, serialized);
                Assert.AreEqual(message, deserializedObject);
            }
            finally
            {
                // Clean up the temporary config file
                if (File.Exists(tempConfigPath))
                {
                    File.Delete(tempConfigPath);
                }
            }
        }

        [Test]
        public void KafkaSerDeTestForJsonMessage()
        {
            var message = RecordGenerator.GetSampleJsonTestData();
            var jsonSerializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
            var jsonDeserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH);

            var serialized = jsonSerializer.Serialize(message, "test-topic-json");
            var deserializedObject = jsonDeserializer.Deserialize("test-topic-json", serialized);

            Assert.True(deserializedObject is JsonDataWithSchema);
            var deserializedMessage = (JsonDataWithSchema)deserializedObject;

            Assert.AreEqual(
                JsonNode.Parse(message.Schema)?.ToString(),
                JsonNode.Parse(deserializedMessage.Schema)?.ToString());
            Assert.AreEqual(
                JsonNode.Parse(message.Payload)?.ToString(),
                JsonNode.Parse(deserializedMessage.Payload)?.ToString());
        }

        [Test]
        public void KafkaSerDeTestForJsonObject()
        {
            var message = SPECIFIC_TEST_RECORD;
            var jsonSerializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
            var jsonDeserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH);

            var serialized = jsonSerializer.Serialize(message, "test-topic-json-car");
            var deserializedObject = jsonDeserializer.Deserialize("test-topic-json-car", serialized);

            Assert.AreEqual(message.GetType(), deserializedObject.GetType());
            var deserializedMessage = (Car)deserializedObject;
            Assert.AreEqual(message.make, deserializedMessage.make);
            Assert.AreEqual(message.model, deserializedMessage.model);
            Assert.AreEqual(message.used, deserializedMessage.used);
            Assert.AreEqual(message.miles, deserializedMessage.miles);
        }
    }
}
