using System;
using System.Collections.Generic;
using Avro;
using Avro.Generic;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.serializer;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.serializer
{
    [TestFixture]
    public class GlueSchemaRegistryAvroSerializerTests
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
        
        [Test]
        public void KafkaSerDeTest()
        {
            var avroRecord = GetTestAvroRecord();
            var configs = new Dictionary<string, dynamic> { { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord } };

            var kafkaSerializer = new GlueSchemaRegistryKafkaSerializer();
            var kafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(configs);
            

            var bytes = kafkaSerializer.Serialize(avroRecord, "test-topic");
            var deserializeObject = kafkaDeserializer.Deserialize("test-topic", bytes);
            
            Assert.IsTrue(deserializeObject is GenericRecord);
            var genericRecord = (GenericRecord)deserializeObject;
            
            Assert.AreEqual(avroRecord,genericRecord);
        }
        
        private static GenericRecord GetTestAvroRecord()
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            user.Add ("name", "Alyssa🌯 🫔 🥗 🥘 🫕 🥫 🍝 🍜 🍲 🍛 🍣 🍱 🥟 🦪 🍤 🍙 🍚 🍘 🍥");
            user.Add("favorite_number", 256);
            user.Add("favorite_color", "blue");
            return user;
        }
    }
}
