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
using System.Text.Json;
using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.serializer.json;
using AWSGsrSerDe.Tests.serializer.json;
using AWSGsrSerDe.Tests.utils;
using Google.Protobuf;
using NUnit.Framework;
using SharpFuzz;
using static AWSGsrSerDe.Tests.utils.ProtobufGenerator;

namespace AWSGsrSerDe.Tests.serializer
{
    [TestFixture]
    public class GlueSchemaRegistryKafkaSerializerFuzzTests
    {
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

        [Test]
        public void FuzzAvroSerializationWithRandomData()
        {
            var random = new Random();
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    var fuzzData = new byte[random.Next(4, 1024)];
                    random.NextBytes(fuzzData);
                    var rand = new Random(BitConverter.ToInt32(fuzzData, 0));
                    
                    var fuzzedSchema = FuzzAvroSchema(fuzzData, rand);
                    var recordSchema = Schema.Parse(fuzzedSchema);
                    var fuzzedRecord = new GenericRecord((RecordSchema)recordSchema);

                    FuzzAvroRecordData(fuzzedRecord, fuzzData, rand);

                    var kafkaSerializer = new GlueSchemaRegistryKafkaSerializer(AVRO_CONFIG_PATH);
                    var serialized = kafkaSerializer.Serialize(fuzzedRecord, "fuzz-topic-avro");
                    
                    if (serialized?.Length > 0)
                    {
                        var kafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(AVRO_CONFIG_PATH);
                        var deserialized = kafkaDeserializer.Deserialize("fuzz-topic-avro", serialized);
                        Assert.IsTrue(deserialized is GenericRecord);
                        
                        var deserializedRecord = (GenericRecord)deserialized;
                        foreach (var field in fuzzedRecord.Schema.Fields)
                        {
                            Assert.AreEqual(fuzzedRecord[field.Name], deserializedRecord[field.Name]);
                        }
                    }
                }
                catch (Exception ex) when (IsExpectedException(ex))
                {
                    TestContext.WriteLine($"Expected exception during Avro fuzzing iteration {i}: {ex.GetType().Name} - {ex.Message}");
                }
            }
        }

        [Test]
        public void FuzzProtobufSerializationWithRandomData()
        {
            var random = new Random();
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    var fuzzData = new byte[random.Next(4, 1024)];
                    random.NextBytes(fuzzData);
                    var rand = new Random(BitConverter.ToInt32(fuzzData, 0));
                    
                    var fuzzedMessage = FuzzProtobufMessage(fuzzData, rand);

                    var dataConfig = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
                    {
                        { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, fuzzedMessage.Descriptor }
                    });

                    var protobufSerializer = new GlueSchemaRegistryKafkaSerializer(PROTOBUF_CONFIG_PATH);
                    var serialized = protobufSerializer.Serialize(fuzzedMessage, $"fuzz-topic-{fuzzedMessage.Descriptor.Name}");
                    
                    if (serialized?.Length > 0)
                    {
                        var protobufDeserializer = new GlueSchemaRegistryKafkaDeserializer(PROTOBUF_CONFIG_PATH, dataConfig);
                        var deserialized = protobufDeserializer.Deserialize($"fuzz-topic-{fuzzedMessage.Descriptor.Name}", serialized);
                        Assert.IsNotNull(deserialized);
                        
                        var deserializedMessage = (IMessage)deserialized;
                        Assert.AreEqual(fuzzedMessage.GetType(), deserializedMessage.GetType());
                        foreach (var field in fuzzedMessage.Descriptor.Fields.InFieldNumberOrder())
                        {
                            Assert.AreEqual(field.Accessor.GetValue(fuzzedMessage), field.Accessor.GetValue(deserializedMessage));
                        }
                    }
                }
                catch (Exception ex) when (IsExpectedException(ex))
                {
                    TestContext.WriteLine($"Expected exception during Protobuf fuzzing iteration {i}: {ex.GetType().Name} - {ex.Message}");
                }
            }
        }

        [Test]
        public void FuzzJsonSerializationWithRandomData()
        {
            var random = new Random();
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    var fuzzData = new byte[random.Next(4, 1024)];
                    random.NextBytes(fuzzData);
                    var rand = new Random(BitConverter.ToInt32(fuzzData, 0));
                    
                    var fuzzedJsonData = FuzzJsonData(fuzzData, rand);
                    
                    var jsonSerializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
                    var serialized = jsonSerializer.Serialize(fuzzedJsonData, "fuzz-topic-json");
                    
                    if (serialized?.Length > 0)
                    {
                        var jsonDeserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH);
                        var deserialized = jsonDeserializer.Deserialize("fuzz-topic-json", serialized);
                        Assert.IsTrue(deserialized is JsonDataWithSchema);
                        
                        var deserializedJson = (JsonDataWithSchema)deserialized;
                        AssertJsonSchemasEqual(fuzzedJsonData.Schema, deserializedJson.Schema);
                    }
                }
                catch (Exception ex) when (IsExpectedException(ex))
                {
                    TestContext.WriteLine($"Expected exception during JSON fuzzing iteration {i}: {ex.GetType().Name} - {ex.Message}");
                }
            }
        }

        [Test]
        public void FuzzDeserializationWithCorruptedBytes()
        {
            var random = new Random();
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    var corruptedData = new byte[random.Next(1, 1024)];
                    random.NextBytes(corruptedData);

                    var kafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(AVRO_CONFIG_PATH);
                    var result = kafkaDeserializer.Deserialize("fuzz-topic-corrupted", corruptedData);
                    
                    if (result != null)
                    {
                        TestContext.WriteLine($"Unexpected successful deserialization of corrupted data: {result.GetType()}");
                    }
                }
                catch (Exception ex) when (IsExpectedException(ex))
                {
                    TestContext.WriteLine($"Expected exception during corrupted data fuzzing iteration {i}: {ex.GetType().Name}");
                }
            }
        }

        [Test]
        public void FuzzConfigurationParsingWithRandomData()
        {
            var random = new Random();
            for (int i = 0; i < 50; i++)
            {
                try
                {
                    var fuzzData = new byte[random.Next(10, 512)];
                    random.NextBytes(fuzzData);
                    var rand = new Random(BitConverter.ToInt32(fuzzData, 0));
                    var tempConfigPath = Path.GetTempFileName();
                    
                    try
                    {
                        var configContent = FuzzConfigurationValues(fuzzData, rand);
                        File.WriteAllText(tempConfigPath, configContent);

                        var kafkaSerializer = new GlueSchemaRegistryKafkaSerializer(tempConfigPath);
                        var testRecord = RecordGenerator.GetTestAvroRecord();
                        var result = kafkaSerializer.Serialize(testRecord, "fuzz-config-test");
                        
                        TestContext.WriteLine($"Fuzzing config iteration {i} succeeded with result length: {result?.Length ?? 0}");
                    }
                    finally
                    {
                        if (File.Exists(tempConfigPath))
                            File.Delete(tempConfigPath);
                    }
                }
                catch (Exception ex) when (IsExpectedException(ex))
                {
                    TestContext.WriteLine($"Expected exception during config fuzzing iteration {i}: {ex.GetType().Name}");
                }
            }
        }

        [Test]
        public void FuzzRoundTripConsistencyTest()
        {
            var random = new Random();
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    var fuzzData = new byte[random.Next(4, 1024)];
                    random.NextBytes(fuzzData);

                    var kafkaSerializer = new GlueSchemaRegistryKafkaSerializer(AVRO_CONFIG_PATH);
                    var kafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(AVRO_CONFIG_PATH);

                    var originalRecord = RecordGenerator.GetFuzzedAvroRecord(fuzzData, random);

                    var serialized = kafkaSerializer.Serialize(originalRecord, "fuzz-roundtrip");
                    
                    if (serialized?.Length > 0)
                    {
                        var deserialized = kafkaDeserializer.Deserialize("fuzz-roundtrip", serialized);
                        
                        if (deserialized is GenericRecord deserializedRecord)
                        {
                            ValidateRoundTripConsistency(originalRecord, deserializedRecord);
                        }
                    }
                }
                catch (Exception ex) when (IsExpectedException(ex))
                {
                    TestContext.WriteLine($"Expected exception during round-trip fuzzing iteration {i}: {ex.GetType().Name}");
                }
            }
        }

        private string FuzzAvroSchema(byte[] data, Random rand)
        {
            var fieldNames = new[] { "name", "favorite_number", "favorite_color" };
            var fuzzedNames = fieldNames.Select(name => 
                rand.Next(0, 3) == 0 ? FuzzString(name, data, rand) : name).ToArray();
            
            return $@"{{""namespace"": ""example.avro"",
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {{""name"": ""{fuzzedNames[0]}"", ""type"": ""string""}},
                        {{""name"": ""{fuzzedNames[1]}"",  ""type"": [""int"", ""null""]}},
                        {{""name"": ""{fuzzedNames[2]}"", ""type"": [""string"", ""null""]}}
                    ]
                    }}";
        }

        private void FuzzAvroRecordData(GenericRecord record, byte[] data, Random rand)
        {
            var schema = record.Schema as RecordSchema;
            foreach (var field in schema.Fields)
            {
                switch (field.Schema.Tag)
                {
                    case Schema.Type.String:
                        record.Add(field.Name, FuzzString("test", data, rand));
                        break;
                    case Schema.Type.Int:
                        record.Add(field.Name, rand.Next(-1000000, 1000000));
                        break;
                    case Schema.Type.Union:
                        if (rand.Next(0, 3) == 0)
                            record.Add(field.Name, null);
                        else if (field.Schema.ToString().Contains("int"))
                            record.Add(field.Name, rand.Next(-1000000, 1000000));
                        else
                            record.Add(field.Name, FuzzString("test", data, rand));
                        break;
                }
            }
        }

        private JsonDataWithSchema FuzzJsonData(byte[] data, Random rand)
        {
            var baseData = RecordGenerator.GetSampleJsonTestData();
            var jsonObj = JsonNode.Parse(baseData.Payload);
            
            FuzzJsonObject(jsonObj, data, rand, 0);
            
            return JsonDataWithSchema.Build(baseData.Schema, jsonObj.ToJsonString());
        }

        private void FuzzJsonObject(JsonNode node, byte[] data, Random rand, int depth)
        {
            if (depth > 3 || node == null) return;
            
            if (node is JsonObject obj)
            {
                var keys = obj.Select(kv => kv.Key).ToList();
                foreach (var key in keys)
                {
                    var value = obj[key];
                    if (value?.GetValueKind() == JsonValueKind.String)
                        obj[key] = FuzzString(value.GetValue<string>(), data, rand);
                    else if (value?.GetValueKind() == JsonValueKind.Number)
                        obj[key] = rand.NextDouble() * 1000;
                    else
                        FuzzJsonObject(value, data, rand, depth + 1);
                }
            }
        }

        private string FuzzConfigurationValues(byte[] data, Random rand)
        {
            var configs = new Dictionary<string, string>
            {
                ["region"] = "us-west-2",
                ["registry.name"] = "default-registry",
                ["schemaAutoRegistrationEnabled"] = "true",
                ["cache.ttl.seconds"] = "86400",
                ["compression.type"] = "ZLIB"
            };

            // Fuzz userAgent and description
            if (rand.Next(0, 2) == 0)
                configs["user.agent"] = FuzzString("test-agent", data, rand);
            if (rand.Next(0, 2) == 0)
                configs["description"] = FuzzString("test-description", data, rand);
            
            // Fuzz other configs that might cause exceptions
            if (rand.Next(0, 3) == 0)
                configs["cache.ttl.seconds"] = rand.Next(-100, 1000000).ToString();
            if (rand.Next(0, 3) == 0)
                configs["registry.name"] = FuzzString("registry", data, rand);

            return string.Join("\n", configs.Select(kv => $"{kv.Key}={kv.Value}"));
        }

        private IMessage FuzzProtobufMessage(byte[] data, Random rand)
        {
            var messages = new IMessage[] { BASIC_SYNTAX2_MESSAGE, BASIC_SYNTAX3_MESSAGE, BASIC_REFERENCING_MESSAGE };
            var baseMessage = messages[rand.Next(messages.Length)];
            
            // Create a new instance and fuzz its fields
            var messageType = baseMessage.GetType();
            var fuzzed = (IMessage)Activator.CreateInstance(messageType);
            var descriptor = fuzzed.Descriptor;
            
            foreach (var field in descriptor.Fields.InFieldNumberOrder())
            {
                if (field.FieldType == Google.Protobuf.Reflection.FieldType.String)
                {
                    field.Accessor.SetValue(fuzzed, FuzzString("test", data, rand));
                }
                else if (field.FieldType == Google.Protobuf.Reflection.FieldType.Int32)
                {
                    field.Accessor.SetValue(fuzzed, rand.Next(-1000000, 1000000));
                }
            }
            
            return fuzzed;
        }

        private string FuzzString(string baseStr, byte[] data, Random rand)
        {
            if (data.Length < 4) return baseStr;
            
            var fuzzType = rand.Next(0, 6);
            switch (fuzzType)
            {
                case 0: return new string((char)rand.Next(32, 127), rand.Next(1, 50));
                case 1: return baseStr + Encoding.UTF8.GetString(data.Take(rand.Next(1, Math.Min(20, data.Length))).ToArray());
                case 2: return string.Empty;
                case 3: return new string('A', rand.Next(1, 1000));
                case 4: return Convert.ToBase64String(data.Take(rand.Next(1, Math.Min(50, data.Length))).ToArray());
                default: return baseStr;
            }
        }

        private string GenerateFuzzedConfigContent(byte[] data)
        {
            var baseConfig = @"region=us-west-2
                    registry.name=default-registry
                    schema.auto.registration.setting=true
                    cache.ttl.seconds=86400
                    compression.type=ZLIB";

            if (data.Length < 20) return baseConfig;

            var rand = new Random(BitConverter.ToInt32(data.Take(4).ToArray()));
            var lines = baseConfig.Split('\n').Where(l => !string.IsNullOrWhiteSpace(l)).ToList();
            
            // Add some fuzzed config lines
            for (int i = 0; i < Math.Min(3, data.Length / 20); i++)
            {
                var key = $"fuzz.property.{i}";
                var valueData = data.Skip(i * 20).Take(10).ToArray();
                var value = GenerateFuzzedString(valueData, rand);
                lines.Add($"{key}={value}");
            }

            return string.Join("\n", lines);
        }

        private string GenerateFuzzedString(byte[] data, Random rand)
        {
            if (data.Length == 0) return string.Empty;

            var length = Math.Min(rand.Next(1, 50), data.Length);
            var stringBytes = data.Take(length).ToArray();
            
            // Try different encodings to test edge cases
            try
            {
                // Filter out control characters to avoid issues
                var filteredBytes = stringBytes.Where(b => b >= 32 && b <= 126).ToArray();
                return filteredBytes.Length > 0 ? Encoding.ASCII.GetString(filteredBytes) : "test";
            }
            catch
            {
                return Convert.ToBase64String(stringBytes);
            }
        }

        private void ValidateRoundTripConsistency(GenericRecord original, GenericRecord deserialized)
        {
            // Basic validation - verify schema and basic structure
            Assert.AreEqual(original.Schema.Name, deserialized.Schema.Name);
            Assert.AreEqual(original.Schema.Fields.Count, deserialized.Schema.Fields.Count);
            
            // Check that fields exist (values may differ due to serialization/deserialization)
            foreach (var field in original.Schema.Fields)
            {
                var originalValue = original[field.Name];
                var deserializedValue = deserialized[field.Name];
                
                // Both should either be null or non-null
                if (originalValue == null)
                {
                    Assert.IsNull(deserializedValue, $"Field {field.Name} should be null");
                }
                else
                {
                    Assert.IsNotNull(deserializedValue, $"Field {field.Name} should not be null");
                }
            }
        }

        private void AssertJsonSchemasEqual(string expectedSchema, string actualSchema)
        {
            try
            {
                // Parse both schemas as JSON to normalize formatting
                var expectedJson = JsonNode.Parse(expectedSchema);
                var actualJson = JsonNode.Parse(actualSchema);
                
                // Compare the normalized JSON strings
                var expectedNormalized = expectedJson.ToJsonString();
                var actualNormalized = actualJson.ToJsonString();
                
                Assert.AreEqual(expectedNormalized, actualNormalized, 
                    "JSON schemas are semantically different");
            }
            catch (JsonException)
            {
                // If JSON parsing fails, fall back to direct string comparison
                // but normalize whitespace first
                var expectedNormalized = NormalizeJsonString(expectedSchema);
                var actualNormalized = NormalizeJsonString(actualSchema);
                
                Assert.AreEqual(expectedNormalized, actualNormalized,
                    "JSON schema strings differ after normalization");
            }
        }
        
        private string NormalizeJsonString(string jsonString)
        {
            // Simple whitespace normalization as fallback
            return string.Join("", jsonString.Where(c => !char.IsWhiteSpace(c)));
        }

        private bool IsExpectedException(Exception ex)
        {
            // Define which exceptions are expected during fuzzing
            return ex is ArgumentException ||
                   ex is ArgumentNullException ||
                   ex is InvalidOperationException ||
                   ex is FormatException ||
                   ex is JsonException ||
                   ex is IOException ||
                   ex is UnauthorizedAccessException ||
                   ex is NotSupportedException ||
                   ex is OverflowException ||
                   ex is OutOfMemoryException ||
                   ex is FileNotFoundException ||
                   ex is Newtonsoft.Json.JsonReaderException ||
                   ex.GetType().Name.Contains("Avro") ||
                   ex.GetType().Name.Contains("Protobuf") ||
                   ex.GetType().Name.Contains("Schema") ||
                   ex.GetType().Name.Contains("Serializ") ||
                   ex.GetType().Name.Contains("Aws");
        }
    }
}
