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

using System.Collections.Generic;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.Tests.utils;
using Google.Protobuf;
using NUnit.Framework;
using static AWSGsrSerDe.Tests.utils.ProtobufGenerator;


namespace AWSGsrSerDe.Tests.memory
{
    [TestFixture]
    public class AwsSchemaRegistryMemeoryLeakDetectionTests
    {
        private const string CONFIG_PATH = "configuration/test-configs/valid-minimal.properties";
        private const string AVRO_DATA_FORMAT = "AVRO";
        private const string JSON_DATA_FORMAT = "JSON";
        private const string PROTOBUF_DATA_FORMAT = "PROTOBUF";

        private static readonly GlueSchemaRegistryKafkaSerializer KafkaSerializer =
            new GlueSchemaRegistryKafkaSerializer(CONFIG_PATH);

        private static readonly GlueSchemaRegistryKafkaDeserializer KafkaDeserializer =
            new GlueSchemaRegistryKafkaDeserializer(CONFIG_PATH);

        private static readonly GlueSchemaRegistryDeserializer Deserializer = new GlueSchemaRegistryDeserializer(CONFIG_PATH);

        private void SerializeDeserializeProtobufMessage()
        {
            var message = (IMessage)BASIC_SYNTAX2_MESSAGE;
            var protobufSerializer = new GlueSchemaRegistryKafkaSerializer(CONFIG_PATH);
            var protobufDeserializer = new GlueSchemaRegistryKafkaDeserializer(CONFIG_PATH);

            var serialized = protobufSerializer.Serialize(message, message.Descriptor.FullName, PROTOBUF_DATA_FORMAT);
            var canDecode = Deserializer.CanDecode(serialized);
            var decodedSchema = Deserializer.DecodeSchema(serialized);
            var deserializedObject = protobufDeserializer.Deserialize(message.Descriptor.FullName, serialized);
        }

        private void SerializeDeserializeJsonMessage()
        {
            var message = RecordGenerator.GetSampleJsonTestData();
            var jsonSerializer = new GlueSchemaRegistryKafkaSerializer(CONFIG_PATH);
            var jsonDeserializer = new GlueSchemaRegistryKafkaDeserializer(CONFIG_PATH);

            var serialized = jsonSerializer.Serialize(message, "test-topic-json", JSON_DATA_FORMAT);
            var canDecode = Deserializer.CanDecode(serialized);
            var decodedSchema = Deserializer.DecodeSchema(serialized);
            var deserializedObject = jsonDeserializer.Deserialize("test-topic-json", serialized);
        }

        private void SerializeDeserializeAvroMessage()
        {
            var message = RecordGenerator.GetTestAvroRecord();

            // Using static KafkaSerializer and KafkaDeserializer which are already configured for AVRO
            var serialized = KafkaSerializer.Serialize(message, "test-topic-avro", AVRO_DATA_FORMAT);
            var canDecode = Deserializer.CanDecode(serialized);
            var decodedSchema = Deserializer.DecodeSchema(serialized);
            var deserializedObject = KafkaDeserializer.Deserialize("test-topic-avro", serialized);
        }

        // TODO: Improve and automate the memory leak check
        // Test for detecting memory leak
        // [Test]
        // public void KafkaSerDeMemoryTestForProtobuf()
        // {
        //     for (;;)
        //     {
        //         SerializeDeserializeAvroMessage();
        //         SerializeDeserializeProtobufMessage();
        //         SerializeDeserializeJsonMessage();
        //     }
        // }
    }
}
