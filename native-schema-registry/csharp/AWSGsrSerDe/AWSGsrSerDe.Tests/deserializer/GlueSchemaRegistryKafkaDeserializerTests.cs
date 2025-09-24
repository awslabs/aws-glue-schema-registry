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
using System.Text.Json.Nodes;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.serializer.avro;
using AWSGsrSerDe.serializer.json;
using AWSGsrSerDe.serializer.protobuf;
using AWSGsrSerDe.Tests.utils;
using Google.Protobuf;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.deserializer
{
    [TestFixture]
    public class GlueSchemaRegistryKafkaDeserializerTests
    {
        private static readonly string CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal.properties");
        private const string AVRO_DATA_FORMAT = "AVRO";
        private const string JSON_DATA_FORMAT = "JSON";
        private const string PROTOBUF_DATA_FORMAT = "PROTOBUF";

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

        private static readonly GlueSchemaRegistryKafkaSerializer KafkaSerializer =
            new GlueSchemaRegistryKafkaSerializer(CONFIG_PATH);

        private static readonly GlueSchemaRegistryKafkaDeserializer KafkaDeserializer =
            new GlueSchemaRegistryKafkaDeserializer(CONFIG_PATH);

        private static readonly GlueSchemaRegistryDeserializer Deserializer = new GlueSchemaRegistryDeserializer(CONFIG_PATH);

        [Test]
        public void TestDeserializerWithMessageEncodedBySerializer_Json()
        {
            var jsonMessage = RecordGenerator.GetSampleJsonTestData();

            // Json Data Encoded
            var jsonSerializer = new GlueSchemaRegistryKafkaSerializer(GetConfigPath("configuration/test-configs/valid-minimal.properties"));
            var bytes = jsonSerializer.Serialize(jsonMessage, "test-topic-json", JSON_DATA_FORMAT);

            Assert.DoesNotThrow(() => Deserializer.CanDecode(bytes));
            Assert.True(Deserializer.CanDecode(bytes));
            Assert.DoesNotThrow(() => Deserializer.DecodeSchema(bytes));
            Assert.DoesNotThrow(() => Deserializer.Decode(bytes));

            var decodeSchema = Deserializer.DecodeSchema(bytes);
            Assert.AreEqual(GlueSchemaRegistryConstants.DataFormat.JSON.ToString(), decodeSchema.DataFormat);
            Assert.AreEqual(
                JsonNode.Parse(jsonMessage.Schema)?.ToJsonString(),
                JsonNode.Parse(decodeSchema.SchemaDef)?.ToJsonString());
        }

        [Test]
        public void TestDeserializerWithMessageNotEncodedBySerializer_Json()
        {
            var jsonMessage = RecordGenerator.GetSampleJsonTestData();
            var jsonSerializer = new JsonSerializer();
            var bytes = jsonSerializer.Serialize(jsonMessage);

            Assert.DoesNotThrow(() => Deserializer.CanDecode(bytes));
            Assert.False(Deserializer.CanDecode(bytes));
            var exception = Assert.Throws(typeof(AwsSchemaRegistryException), () => Deserializer.DecodeSchema(bytes));
            Assert.IsTrue(exception.Message.StartsWith("Invalid schema registry header version byte in data"));
            exception = Assert.Throws(typeof(AwsSchemaRegistryException), () => Deserializer.Decode(bytes));
            Assert.IsTrue(exception.Message.StartsWith("Invalid schema registry header version byte in data"));

        }

        [Test]
        public void TestDeserializerWithMessageEncodedBySerializer_Avro()
        {
            var avroRecord = RecordGenerator.GetTestAvroRecord();

            // Avro Data encoded - using the static KafkaSerializer which is already configured for AVRO
            var bytes = KafkaSerializer.Serialize(avroRecord, "test-topic-avro", AVRO_DATA_FORMAT);

            Assert.DoesNotThrow(() => Deserializer.CanDecode(bytes));
            Assert.True(Deserializer.CanDecode(bytes));
            Assert.DoesNotThrow(() => Deserializer.DecodeSchema(bytes));
            Assert.DoesNotThrow(() => Deserializer.Decode(bytes));

            var decodeSchema = Deserializer.DecodeSchema(bytes);
            Assert.AreEqual(GlueSchemaRegistryConstants.DataFormat.AVRO.ToString(), decodeSchema.DataFormat);
            Assert.AreEqual(avroRecord.Schema.ToString(), decodeSchema.SchemaDef);
        }

        [Test]
        public void TestDeserializerWithMessageNotEncodedBySerializer_Avro()
        {
            var avroRecord = RecordGenerator.GetTestAvroRecord();

            var avroSerializer = new AvroSerializer();
            var bytes = avroSerializer.Serialize(avroRecord);

            Assert.DoesNotThrow(() => Deserializer.CanDecode(bytes));
            Assert.False(Deserializer.CanDecode(bytes));
            var exception = Assert.Throws(typeof(AwsSchemaRegistryException), () => Deserializer.DecodeSchema(bytes));
            Console.WriteLine(exception.Message);
            Assert.IsTrue(exception.Message.StartsWith("Invalid schema registry header version byte in data"));
            exception = Assert.Throws(typeof(AwsSchemaRegistryException), () => Deserializer.Decode(bytes));
            Assert.IsTrue(exception.Message.StartsWith("Invalid schema registry header version byte in data"));
        }

        [Test]
        public void TestDeserializerWithMessageEncodedBySerializer_Protobuf()
        {
            var protobufMessage = (IMessage)ProtobufGenerator.BASIC_REFERENCING_MESSAGE;

            // Protobuf Data encoded
            var protobufSerializer = new GlueSchemaRegistryKafkaSerializer(CONFIG_PATH);
            var bytes = protobufSerializer.Serialize(protobufMessage, protobufMessage.Descriptor.FullName, PROTOBUF_DATA_FORMAT);

            Assert.DoesNotThrow(() => Deserializer.CanDecode(bytes));
            Assert.True(Deserializer.CanDecode(bytes));
            Assert.DoesNotThrow(() => Deserializer.DecodeSchema(bytes));
            Assert.DoesNotThrow(() => Deserializer.Decode(bytes));

            var decodeSchema = Deserializer.DecodeSchema(bytes);
            Assert.AreEqual(GlueSchemaRegistryConstants.DataFormat.PROTOBUF.ToString(), decodeSchema.DataFormat);
        }

        [Test]
        public void TestDeserializerWithMessageNotEncodedBySerializer_Protobuf()
        {
            var protobufMessage = (IMessage)ProtobufGenerator.BASIC_REFERENCING_MESSAGE;

            var protobufSerializer = new ProtobufSerializer();
            var bytes = protobufSerializer.Serialize(protobufMessage);

            Assert.DoesNotThrow(() => Deserializer.CanDecode(bytes));
            Assert.False(Deserializer.CanDecode(bytes));
            var exception = Assert.Throws(typeof(AwsSchemaRegistryException), () => Deserializer.DecodeSchema(bytes));
            Assert.IsTrue(exception.Message.StartsWith("Data is not compatible with schema registry"));
            exception = Assert.Throws(typeof(AwsSchemaRegistryException), () => Deserializer.Decode(bytes));
            Assert.IsTrue(exception.Message.StartsWith("Data is not compatible with schema registry"));
        }

        /// <summary>
        /// Tests that the constructor works with null dataConfig parameter
        /// </summary>
        [Test]
        public void TestDeserializerConstructor_WithNullDataConfig()
        {
            // Constructor should work exactly as before with null dataConfig
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(CONFIG_PATH, null);
            Assert.IsNotNull(deserializer);
        }
    }
}
