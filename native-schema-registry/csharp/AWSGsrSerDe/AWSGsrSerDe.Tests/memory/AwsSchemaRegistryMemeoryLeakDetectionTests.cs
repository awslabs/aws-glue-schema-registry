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
        private static readonly Dictionary<string, dynamic> Configs = new Dictionary<string, dynamic>
        {
            { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord },
            { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.AVRO },
        };

        private static readonly GlueSchemaRegistryKafkaSerializer KafkaSerializer =
            new GlueSchemaRegistryKafkaSerializer(Configs);

        private static readonly GlueSchemaRegistryKafkaDeserializer KafkaDeserializer =
            new GlueSchemaRegistryKafkaDeserializer(Configs);

        private static readonly GlueSchemaRegistryDeserializer Deserializer = new GlueSchemaRegistryDeserializer();

        private void SerializeDeserializeProtobufMessage()
        {
            var message = (IMessage)BASIC_SYNTAX2_MESSAGE;
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, message.Descriptor },
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.PROTOBUF },
            };

            KafkaSerializer.Configure(configs);
            KafkaDeserializer.Configure(configs);

            var serialized = KafkaSerializer.Serialize(message, message.Descriptor.FullName);
            var canDecode = Deserializer.CanDecode(serialized);
            var decodedSchema = Deserializer.DecodeSchema(serialized);
            var deserializedObject =
                KafkaDeserializer.Deserialize(message.Descriptor.FullName, serialized);
        }

        private void SerializeDeserializeJsonMessage()
        {
            var message = RecordGenerator.GetSampleJsonTestData();
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.JSON },
            };

            KafkaSerializer.Configure(configs);
            KafkaDeserializer.Configure(configs);

            var serialized = KafkaSerializer.Serialize(message, "test-topic-json");
            var canDecode = Deserializer.CanDecode(serialized);
            var decodedSchema = Deserializer.DecodeSchema(serialized);
            var deserializedObject = KafkaDeserializer.Deserialize("test-topic-json", serialized);
        }

        private void SerializeDeserializeAvroMessage()
        {
            var message = RecordGenerator.GetTestAvroRecord();

            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord },
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.AVRO },
            };

            KafkaSerializer.Configure(configs);
            KafkaDeserializer.Configure(configs);

            var serialized = KafkaSerializer.Serialize(message, "test-topic-avro");
            var canDecode = Deserializer.CanDecode(serialized);
            var decodedSchema = Deserializer.DecodeSchema(serialized);
            var deserializedObject = KafkaDeserializer.Deserialize("test-topic-avro", serialized);
        }

        // TODO: Improve and automate the memory leak check
        // Test for detecting memory leak
        [Test]
        public void KafkaSerDeMemoryTestForProtobuf()
        {
            for (;;)
            {
                SerializeDeserializeAvroMessage();
                SerializeDeserializeProtobufMessage();
                SerializeDeserializeJsonMessage();
            }
        }
    }
}
