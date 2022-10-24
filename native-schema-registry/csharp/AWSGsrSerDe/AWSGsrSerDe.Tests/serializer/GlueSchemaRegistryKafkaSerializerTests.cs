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
using Avro;
using Avro.Generic;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.serializer;
using Google.Protobuf;
using NUnit.Framework;
using static AWSGsrSerDe.Tests.utils.ProtobufGenerator;

namespace AWSGsrSerDe.Tests.serializer
{
    [TestFixture]
    public class GlueSchemaRegistryKafkaSerializerTests
    {
        private const string TestAvroSchema = "{\"namespace\": \"example.avro\",\n"
                                              + " \"type\": \"record\",\n"
                                              + " \"name\": \"User\",\n"
                                              + " \"fields\": [\n"
                                              + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                                              + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
                                              + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
                                              + " ]\n"
                                              + "}";
        
        private static readonly Dictionary<string, dynamic> Configs = new Dictionary<string, dynamic>
        {
            { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord },
            { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.AVRO },
        };

        private static readonly GlueSchemaRegistryKafkaSerializer KafkaSerializer = new GlueSchemaRegistryKafkaSerializer(Configs);
        private static readonly GlueSchemaRegistryKafkaDeserializer KafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(Configs);

        [Test]
        public void KafkaSerDeTestForAvroGenericRecord()
        {
            var avroRecord = GetTestAvroRecord();
            
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord },
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.AVRO },
            };
            
            KafkaSerializer.Configure(configs);
            KafkaDeserializer.Configure(configs);

            var bytes = KafkaSerializer.Serialize(avroRecord, "test-topic");
            var deserializeObject = KafkaDeserializer.Deserialize("test-topic", bytes);

            Assert.IsTrue(deserializeObject is GenericRecord);
            var genericRecord = (GenericRecord)deserializeObject;

            Assert.AreEqual(avroRecord, genericRecord);
        }

        private static GenericRecord GetTestAvroRecord()
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            user.Add("name", "Alyssa🌯 🫔 🥗 🥘 🫕 🥫 🍝 🍜 🍲 🍛 🍣 🍱 🥟 🦪 🍤 🍙 🍚 🍘 🍥");
            user.Add("favorite_number", 256);
            user.Add("favorite_color", "blue");
            return user;
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
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, message.Descriptor },
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.PROTOBUF },
            };

            KafkaSerializer.Configure(configs);
            KafkaDeserializer.Configure(configs);

            var serialized = KafkaSerializer.Serialize(message, message.Descriptor.FullName);

            var deserializedObject =
                KafkaDeserializer.Deserialize(message.Descriptor.FullName, serialized);
            Assert.AreEqual(message, deserializedObject);
        }


        // TODO: Improve and automate the memory leak check
        // Test for detecting memory leak
        // [Test]
        // [TestCaseSource(nameof(TestMessageProvider))]
        // public void KafkaSerDeTestForAllProtobufTypesMemCheck(IMessage message)
        // {
        //     for (; ;)
        //     {
        //         KafkaSerDeTestForAllProtobufTypes(message);
        //     }
        // }
    }
}
