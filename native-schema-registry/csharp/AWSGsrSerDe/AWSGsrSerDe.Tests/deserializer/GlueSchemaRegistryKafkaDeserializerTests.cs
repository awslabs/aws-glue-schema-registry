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

        [Test]
        public void TestDeserializerWithMessageEncodedBySerializer_Json()
        {
            var jsonMessage = RecordGenerator.GetSampleJsonTestData();

            // Json Data Encoded
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.JSON },
            };

            KafkaSerializer.Configure(configs);
            var bytes = KafkaSerializer.Serialize(jsonMessage, "test-topic-json");

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

            // Avro Data encoded
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord },
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.AVRO },
            };

            KafkaSerializer.Configure(configs);
            var bytes = KafkaSerializer.Serialize(avroRecord, "test-topic-avro");

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
            var configs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, protobufMessage.Descriptor },
                { GlueSchemaRegistryConstants.DataFormatType, GlueSchemaRegistryConstants.DataFormat.PROTOBUF },
            };

            KafkaSerializer.Configure(configs);

            var bytes = KafkaSerializer.Serialize(protobufMessage, protobufMessage.Descriptor.FullName);

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
    }
}
