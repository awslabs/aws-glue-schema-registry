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

        private static readonly Dictionary<string, dynamic> Configs = new Dictionary<string, dynamic>
        {
            { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord },
            { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.AVRO },
        };

        private static readonly GlueSchemaRegistryKafkaSerializer KafkaSerializer =
            new GlueSchemaRegistryKafkaSerializer(Configs);

        private static readonly GlueSchemaRegistryKafkaDeserializer KafkaDeserializer =
            new GlueSchemaRegistryKafkaDeserializer(Configs);


        [Test]
        public void KafkaSerDeTestForAvroGenericRecord()
        {
            var avroRecord = RecordGenerator.GetTestAvroRecord();

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

        [Test]
        public void KafkaSerDeTestForJsonMessage()
        {
            var message = RecordGenerator.GetSampleJsonTestData();
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.JSON },
            };

            KafkaSerializer.Configure(configs);
            KafkaDeserializer.Configure(configs);

            var serialized = KafkaSerializer.Serialize(message, "test-topic-json");
            var deserializedObject = KafkaDeserializer.Deserialize("test-topic-json", serialized);

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
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.JsonObjectType, message.GetType() },
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.JSON },
            };

            KafkaSerializer.Configure(configs);
            KafkaDeserializer.Configure(configs);

            var serialized = KafkaSerializer.Serialize(message, "test-topic-json-car");
            var deserializedObject = KafkaDeserializer.Deserialize("test-topic-json-car", serialized);

            Assert.AreEqual(message.GetType(), deserializedObject.GetType());
            var deserializedMessage = (Car)deserializedObject;
            Assert.AreEqual(message.make, deserializedMessage.make);
            Assert.AreEqual(message.model, deserializedMessage.model);
            Assert.AreEqual(message.used, deserializedMessage.used);
            Assert.AreEqual(message.miles, deserializedMessage.miles);
        }
    }
}
