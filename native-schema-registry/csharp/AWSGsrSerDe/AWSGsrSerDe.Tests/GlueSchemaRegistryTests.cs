using NUnit.Framework;
using System.IO;
using Avro;
using Avro.Generic;
using Avro.IO;

namespace AWSGsrSerDe.Tests
{
    [TestFixture]
    public class BasicSerDeTest
    {

        private GlueSchemaRegistrySerializer _serializer;
        private GlueSchemaRegistryDeserializer _deserializer;
        private GlueSchemaRegistrySchema _schema;

        private const string TestAvroSchema = "{\"namespace\": \"example.avro\",\n"
                                              + " \"type\": \"record\",\n"
                                              + " \"name\": \"User\",\n"
                                              + " \"fields\": [\n"
                                              + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                                              + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
                                              + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
                                              + " ]\n"
                                              + "}";

        private byte[] _data;

        [SetUp]
        public void Setup()
        {
            _serializer = new GlueSchemaRegistrySerializer();
            _deserializer = new GlueSchemaRegistryDeserializer();
            
            _schema = new GlueSchemaRegistrySchema("TestSchemaName", TestAvroSchema, "AVRO");
            _data = GetAvroMessage();
        }

        [Test]
        public void AssertSerDe()
        {
            var output = _serializer.Encode("TestTransportName", _schema, _data);

            Assert.IsNotEmpty(output);
            Assert.True(_deserializer.CanDecode(output));

            var decoded = _deserializer.Decode(output);
            var deserializedSchema = _deserializer.DecodeSchema(output);

            Assert.AreEqual(_schema, deserializedSchema);
            
            Assert.IsNotEmpty(decoded);
            Assert.AreEqual(_data.Length, decoded.Length);

            for (var index = 0; index < _data.Length; index++)
            {
                Assert.AreEqual(_data[index], decoded[index]);
            }
        }

        private static byte[] GetAvroMessage()
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            user.Add ("name", "Alyssa");
            user.Add("favorite_number", 256);
            user.Add("favorite_color", "blue");

            var genericDatumWriter = new  GenericDatumWriter<GenericRecord>(recordSchema);
            var encoded = new MemoryStream();
            var encoder = new BinaryEncoder(encoded);
            genericDatumWriter.Write(user, encoder);
            encoder.Flush();

            return encoded.ToArray();
        }
    }
}