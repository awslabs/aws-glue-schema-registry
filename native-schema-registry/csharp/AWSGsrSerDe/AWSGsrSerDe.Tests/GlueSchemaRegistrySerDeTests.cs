using System;
using System.Collections.Generic;
using NUnit.Framework;
using System.IO;
using Avro;
using Avro.Generic;
using Avro.IO;

namespace AWSGsrSerDe.Tests
{
    [TestFixture]
    public class GlueSchemaRegistrySerDeTests
    {

        private static GlueSchemaRegistrySerializer _serializer;
        private static GlueSchemaRegistryDeserializer _deserializer;
        private static GlueSchemaRegistrySchema _schema;

        private const string TransportName = "SomeTransportName";
        private const string TestAvroSchema = "{\"namespace\": \"example.avro\",\n"
                                              + " \"type\": \"record\",\n"
                                              + " \"name\": \"User\",\n"
                                              + " \"fields\": [\n"
                                              + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                                              + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
                                              + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
                                              + " ]\n"
                                              + "}";

        private static byte[] _data;

        [SetUp]
        public void Setup()
        {
            _serializer = new GlueSchemaRegistrySerializer("configuration/test-configs/valid-minimal.properties");
            _deserializer = new GlueSchemaRegistryDeserializer("configuration/test-configs/valid-minimal.properties");
            
            _schema = new GlueSchemaRegistrySchema("TestSchemaName", TestAvroSchema, "AVRO");
            _data = GetAvroMessage();
        }

        [TearDown]
        public void TearDown()
        {
            _serializer.Dispose();
            _deserializer.Dispose();
        }

        [Test]
        public void SerDe_Successfully_Encodes_Decodes_Records()
        {
                
            var output = _serializer.Encode(TransportName, _schema, _data);

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

            var decodedAvroRecord = DecodeAvroMessage(deserializedSchema.SchemaDef, decoded);
            Assert.AreEqual(GetTestAvroRecord(), decodedAvroRecord);
        }

        [Test]
        public void Exceptions_Are_Thrown_By_Serializer()
        {
            var invalidSchema = new GlueSchemaRegistrySchema("someName", "{}", "InvalidFormat");

            var actual = Assert.Throws(typeof(AwsSchemaRegistryException),
                () => _serializer.Encode(TransportName, invalidSchema, _data));
            
            Assert.True(actual.Message.Contains("No enum constant software.amazon.awssdk.services.glue.model.DataFormat.InvalidFormat"));
        }
        
        [Test]
        public void Exceptions_Are_Thrown_By_Serializer_ForNullEmpty()
        {
            var invalidSchema = new GlueSchemaRegistrySchema(null, "{}", "InvalidFormat");

            var ex = Assert.Throws(typeof(ArgumentOutOfRangeException),
                () => _serializer.Encode(TransportName, invalidSchema, _data));
            Assert.AreEqual("Schema passed cannot be null", ex.Message);
            
            invalidSchema = new GlueSchemaRegistrySchema("someName", null, "InvalidFormat");

            ex = Assert.Throws(typeof(ArgumentOutOfRangeException),
                () => _serializer.Encode(TransportName, invalidSchema, _data));
            Assert.AreEqual("Schema passed cannot be null", ex.Message);
            
            invalidSchema = new GlueSchemaRegistrySchema("someName", "{}", null);

            ex = Assert.Throws(typeof(ArgumentOutOfRangeException),
                () => _serializer.Encode(TransportName, invalidSchema, _data));
            Assert.AreEqual("Schema passed cannot be null", ex.Message);
            
            ex = Assert.Throws(typeof(ArgumentException),
                () => _serializer.Encode("", _schema, null));
            Assert.AreEqual("bytes is null or empty (Parameter 'bytes')", ex.Message);

            var empty = Array.Empty<byte>();
            ex = Assert.Throws(typeof(ArgumentException),
                () => _serializer.Encode(TransportName, _schema, empty));
            Assert.AreEqual("bytes is null or empty (Parameter 'bytes')", ex.Message);
        }

        private static List<Func<byte[], object>> DeserializerNullTestCases()
        {
            var empty = Array.Empty<byte>();
            return new List<Func<byte[], object>>
            {
                _ => _deserializer.Decode(null),
                _ => _deserializer.Decode(empty),
                _ => _deserializer.DecodeSchema(null),
                _ => _deserializer.DecodeSchema(empty),
                _ => _deserializer.CanDecode(null),
                _ => _deserializer.CanDecode(empty)
            };
        }

        [Test, TestCaseSource(nameof(DeserializerNullTestCases))]
        public void Exceptions_Are_Thrown_By_DeSerializer_ForNullEmpty(Func<byte[], object> deserializerFunc)
        {
            var ex = Assert.Throws(typeof(ArgumentException), () => deserializerFunc.Invoke(null));
            Assert.AreEqual("Encoded bytes is null or Empty. (Parameter 'encoded')", ex.Message);
        }

        [Test]
        public void Exceptions_Are_Thrown_By_DeSerializer()
        {
            var actual = Assert.Throws(typeof(AwsSchemaRegistryException),
                () => _deserializer.Decode(new byte[10]));
            
            Assert.True(actual.Message.StartsWith("Data is not compatible with schema registry size:"));
            
            actual = Assert.Throws(typeof(AwsSchemaRegistryException),
                () => _deserializer.DecodeSchema(new byte[10]));
            
            Assert.True(actual.Message.StartsWith("Data is not compatible with schema registry size:"));

            //This method never throws Exception ideally but adding it for coverage here.
            Assert.False(_deserializer.CanDecode(new byte[100]));
        }

        // TODO: Not so reliable way to test for memory leaks. Valgrind works for dotnet, needs to be
        // automated during build.
//          [Test]
//          public void TestMemoryLeaks(bool infinite)
//          {
//              while (true)
//              {
//                  SerDe_Successfully_Encodes_Decodes_Records();
//                  Exceptions_Are_Thrown_By_Serializer();
//                  Exceptions_Are_Thrown_By_DeSerializer();
//                  if (!infinite)
//                  {
//                      break;
//                  }
//              }
//          }

        private static GenericRecord GetTestAvroRecord()
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            user.Add ("name", "Alyssa🌯 🫔 🥗 🥘 🫕 🥫 🍝 🍜 🍲 🍛 🍣 🍱 🥟 🦪 🍤 🍙 🍚 🍘 🍥");
            user.Add("favorite_number", 256);
            user.Add("favorite_color", "blue");
            return user;
        }
        private static byte[] GetAvroMessage()
        {
            var user = GetTestAvroRecord();

            var genericDatumWriter = new  GenericDatumWriter<GenericRecord>(user.Schema);
            var encoded = new MemoryStream();
            var encoder = new BinaryEncoder(encoded);
            genericDatumWriter.Write(user, encoder);
            encoder.Flush();

            return encoded.ToArray();
        }
        
        private static GenericRecord DecodeAvroMessage(String writerSchemaText, byte[] bytes)
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var writerSchema = Schema.Parse(writerSchemaText);
            var genericDatumReader = new GenericDatumReader<GenericRecord>(writerSchema, recordSchema);
            var decoded = new MemoryStream(bytes);
            var decoder = new BinaryDecoder(decoded);
            var decodedUser = genericDatumReader.Read(null, decoder);

            return decodedUser;
        }
    }
}
