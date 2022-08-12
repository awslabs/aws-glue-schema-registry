using System.Collections.Generic;
using Avro;
using Avro.Generic;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer.avro;
using AWSGsrSerDe.serializer.avro;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.serializer.avro
{
    [TestFixture]
    public class AvroSerializerTest
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

        private User _userDefinedSpecificRecord;
        private GlueSchemaRegistrySchema _userDefinedSpecificRecordSchema;
        
        private GenericRecord _userDefinedGenericRecord;
        private GlueSchemaRegistrySchema _userDefinedGenericRecordSchema;

        private AvroDeserializer _avroSpecificDeserializer;
        private AvroDeserializer _avroGenericDeserializer;


        [SetUp]
        public void Setup()
        {
            SetupAvroSpecificRecord();
            SetupAvroGenericRecord();
            SetupAvroDeserializers();
        }

        private void SetupAvroSpecificRecord()
        {
            var user = new User
            {
                Name = "stray",
                FavoriteNumber = 2022,
                FavoriteColor = "orange"
            };
            _userDefinedSpecificRecord = user;
            _userDefinedSpecificRecordSchema = new GlueSchemaRegistrySchema("test",
                user.Schema.ToString(),
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString()
            );
        }
        
        private void SetupAvroGenericRecord()
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);
            
            user.Add("name","sansa");
            user.Add("favorite_number", 99);
            user.Add("favorite_color", "red");
            
            _userDefinedGenericRecordSchema = new GlueSchemaRegistrySchema("test",
                TestAvroSchema,
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString()
            );

            _userDefinedGenericRecord = user;
        }

        private void SetupAvroDeserializers()
        {
            var genericDeserializerConfigs = new Dictionary<string,dynamic>
            {
                { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord }
            };
            var specificDeserializerConfigs = new Dictionary<string,dynamic>
            {
                { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.SpecificRecord }
            };

            var genericConfiguration = new GlueSchemaRegistryConfiguration(genericDeserializerConfigs);
            _avroGenericDeserializer = new AvroDeserializer(genericConfiguration);
            
            var specificConfiguration = new GlueSchemaRegistryConfiguration(specificDeserializerConfigs);
            _avroSpecificDeserializer = new AvroDeserializer(specificConfiguration);
            
        }
        
        [Test]
        public void WhenSerializeIsCalledReturnsCachedInstance()
        {
            var avroSerializer = new AvroSerializer();

            avroSerializer.Serialize(_userDefinedGenericRecord);
            avroSerializer.Serialize(_userDefinedSpecificRecord);

            Assert.AreEqual(2, avroSerializer.CacheSize);
        }

        [Test]
        public void TestSerialize_NullData_ReturnsNull()
        {
            var avroSerializer = new AvroSerializer();
            Assert.IsNull(avroSerializer.Serialize(null));
        }

        [Test]
        public void TestSerialize_SpecificRecord_Succeed()
        {
            var avroSerializer = new AvroSerializer();
            var serializedResult = avroSerializer.Serialize(_userDefinedSpecificRecord);

            Assert.NotNull(serializedResult);

            var deserialize =  _avroSpecificDeserializer.Deserialize(serializedResult,
                _userDefinedSpecificRecordSchema);
            Assert.True(deserialize is User);

            var deserializedUser = (User) deserialize;

            Assert.AreEqual(_userDefinedSpecificRecord.Name,deserializedUser.Name);
            Assert.AreEqual(_userDefinedSpecificRecord.FavoriteNumber,deserializedUser.FavoriteNumber);
            Assert.AreEqual(_userDefinedSpecificRecord.FavoriteColor,deserializedUser.FavoriteColor);

        }
        
        [Test]
        public void TestSerialize_GenericRecord_Succeed()
        {
            var avroSerializer = new AvroSerializer();
            var serializedResult = avroSerializer.Serialize(_userDefinedGenericRecord);

            Assert.NotNull(serializedResult);

            var deserialize = _avroGenericDeserializer.Deserialize(serializedResult,
                _userDefinedGenericRecordSchema);
            Assert.AreEqual(_userDefinedGenericRecord,deserialize);
        }
        
        /*
         * Test Invalid types 
         */
        
        [Test]
        public void TestSerialize_UnknownRecordType_ThrowsException()
        {
            const string stringObject = "string";
            var avroSerializer = new AvroSerializer();

            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => avroSerializer.Serialize(stringObject));
            Assert.AreEqual("Unsupported Avro Data formats", exception.Message);
        }
    }
}