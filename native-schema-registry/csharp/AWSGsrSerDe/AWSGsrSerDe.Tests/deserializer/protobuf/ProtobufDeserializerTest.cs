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
using System.IO;
using System.Linq;
using System.Text;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer.protobuf;
using AWSGsrSerDe.serializer.protobuf;
using Com.Amazonaws.Services.Schemaregistry.Tests.Protobuf.Syntax2.Basic;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using NUnit.Framework;
using static AWSGsrSerDe.common.GlueSchemaRegistryConstants.DataFormat;
using static AWSGsrSerDe.Tests.utils.ProtobufGenerator;

namespace AWSGsrSerDe.Tests.deserializer.protobuf
{
    [TestFixture]
    public class ProtobufDeserializerTest
    {
        [Test]
        public void TestConstruct_NullArg_ThrowsException()
        {
            var ex = Assert.Throws(typeof(ArgumentNullException), () => new ProtobufDeserializer(null));
            Assert.AreEqual("Value cannot be null. (Parameter 'config')", ex.Message);
        }

        [Test]
        public void TestConstruct_ValidArg_Succeed()
        {
            var config = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, Phone.Descriptor },
            });
            var protobufDeserializer = new ProtobufDeserializer(config);
            Assert.NotNull(protobufDeserializer);
        }

        [Test]
        public void TestDeserialize_NullArgs_ThrowsException()
        {
            var config = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, Phone.Descriptor },
            });
            var protobufSerializer = new ProtobufSerializer();
            var protobufDeserializer = new ProtobufDeserializer(config);
            var schema = new GlueSchemaRegistrySchema(
                "Basic",
                protobufSerializer.GetSchemaDefinition(BASIC_SYNTAX2_MESSAGE),
                PROTOBUF.ToString());
            var ex = Assert.Throws(typeof(ArgumentNullException), () => protobufDeserializer.Deserialize(null, schema));
            Assert.AreEqual("Value cannot be null. (Parameter 'data')", ex.Message);
            ex = Assert.Throws(
                typeof(ArgumentNullException),
                () => protobufDeserializer.Deserialize(protobufSerializer.Serialize(BASIC_SYNTAX2_MESSAGE), null));
            Assert.AreEqual("Value cannot be null. (Parameter 'schema')", ex.Message);
        }

        private static List<IMessage> TestDeserializationMessageProvider()
        {
            return new List<IMessage>
            {
                BASIC_SYNTAX2_MESSAGE,
                BASIC_SYNTAX3_MESSAGE,
                BASIC_REFERENCING_MESSAGE,
                DOTNET_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE,
                NESTING_MESSAGE_PROTO2,
                NESTING_MESSAGE_PROTO3,
                SNAKE_CASE_MESSAGE,
                ANOTHER_SNAKE_CASE_MESSAGE,
                DOLLAR_SYNTAX_3_MESSAGE,
                HYPHEN_ATED_PROTO_FILE_MESSAGE,
                DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE,
                UNICODE_MESSAGE,
                CONFLICTING_NAME_MESSAGE,
                NESTED_CONFLICTING_NAME_MESSAGE,
                NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
            };
        }

        [Test]
        [TestCaseSource(nameof(TestDeserializationMessageProvider))]
        public void TestDeserialize_Succeed_ForAllTypesOfMessages(IMessage message)
        {
            // Use hybrid approach: load base config from properties file and add dynamic protobuf descriptor
            var config = CreateProtobufTestConfig(message.Descriptor);
            var protobufSerializer = new ProtobufSerializer();
            var serializedBytes = protobufSerializer.Serialize(message);

            var protobufDeserializer = new ProtobufDeserializer(config);
            var deserializeMessage = protobufDeserializer.Deserialize(
                serializedBytes,
                new GlueSchemaRegistrySchema(message.Descriptor.Name, "dummy schema def", PROTOBUF.ToString()));
            Assert.AreEqual(message, deserializeMessage);
        }

        [Test]
        public void TestDeserialize_InvalidBytes_ThrowsException()
        {
            var config = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, Phone.Descriptor },
            });
            const string invalid = "invalid";
            var protobufDeserializer = new ProtobufDeserializer(config);
            var ex = Assert.Throws(typeof(AwsSchemaRegistryException), () => protobufDeserializer.Deserialize(
                Encoding.Default.GetBytes(invalid),
                new GlueSchemaRegistrySchema("dummy", "dummy schema def", PROTOBUF.ToString())));
            Assert.AreEqual("Exception occurred while de-serializing Protobuf message", ex.Message);
        }

        /// <summary>
        /// Finds the project root by looking for .csproj file and returns absolute path to config file.
        /// This method is borrowed from ConfigFileValidationTests to maintain consistency with other tests.
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
        /// Parses a Java-style properties file into a dictionary.
        /// This method handles the basic key=value format used by the test configuration files.
        /// </summary>
        /// <param name="filePath">Path to the properties file</param>
        /// <returns>Dictionary containing the parsed key-value pairs</returns>
        private static Dictionary<string, dynamic> ParsePropertiesFile(string filePath)
        {
            var properties = new Dictionary<string, dynamic>();
            
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"Properties file not found: {filePath}");
            }

            foreach (var line in File.ReadAllLines(filePath))
            {
                var trimmedLine = line.Trim();
                
                // Skip empty lines and comments
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#") || trimmedLine.StartsWith("//"))
                {
                    continue;
                }

                var delimiterIndex = trimmedLine.IndexOf('=');
                if (delimiterIndex > 0)
                {
                    var key = trimmedLine.Substring(0, delimiterIndex).Trim();
                    var value = trimmedLine.Substring(delimiterIndex + 1).Trim();
                    
                    // Parse common value types
                    if (bool.TryParse(value, out var boolValue))
                    {
                        properties[key] = boolValue;
                    }
                    else if (int.TryParse(value, out var intValue))
                    {
                        properties[key] = intValue;
                    }
                    else
                    {
                        properties[key] = value;
                    }
                }
            }

            return properties;
        }

        /// <summary>
        /// Creates a protobuf test configuration by combining file-based config with dynamic protobuf descriptor.
        /// 
        /// This hybrid approach is necessary because:
        /// 1. The move to config file approach broke protobuf tests since each test needs a different MessageDescriptor
        /// 2. Config files are static and cannot specify different protobuf descriptors per test case
        /// 3. This method maintains compatibility with the config file approach while enabling dynamic protobuf configuration
        /// 4. It loads base settings (region, registry name, etc.) from the properties file and adds the test-specific descriptor
        /// </summary>
        /// <param name="descriptor">The protobuf message descriptor for this specific test case</param>
        /// <returns>GlueSchemaRegistryDataFormatConfiguration with both file-based and dynamic settings</returns>
        private GlueSchemaRegistryDataFormatConfiguration CreateProtobufTestConfig(MessageDescriptor descriptor)
        {
            // Load base configuration from the protobuf properties file
            var configPath = GetConfigPath("configuration/test-configs/valid-minimal-protobuf.properties");
            var baseConfigDict = ParsePropertiesFile(configPath);
            
            // Add the dynamic protobuf descriptor that each test case requires
            // This is the critical missing piece that caused the original test failures
            baseConfigDict[GlueSchemaRegistryConstants.ProtobufMessageDescriptor] = descriptor;
            
            return new GlueSchemaRegistryDataFormatConfiguration(baseConfigDict);
        }
    }
}
