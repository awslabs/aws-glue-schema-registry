namespace AWSGsrSerDe.Tests.deserializer.avro
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using Avro;
    using Avro.Generic;
    using Avro.IO;
    using AWSGsrSerDe.common;
    using AWSGsrSerDe.deserializer.avro;
    using AWSGsrSerDe.serializer.avro;
    using AWSGsrSerDe.Tests.utils;
    using NUnit.Framework;
    using Encoder = Avro.IO.Encoder;

    [TestFixture]
    public class AvroDeserializerTest
    {
        private const string TestAvroSchema = "{\"namespace\": \"AWSGsrSerDe.Tests\",\n"
                                              + " \"type\": \"record\",\n"
                                              + " \"name\": \"User\",\n"
                                              + " \"fields\": [\n"
                                              + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                                              + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
                                              + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
                                              + " ]\n"
                                              + "}";
        
        private static readonly Dictionary<string, Schema> AvroSchemas = SchemaLoader.LoadAllAvroSchemas();

        private static readonly GlueSchemaRegistrySchema GenericRecordSchema = new GlueSchemaRegistrySchema(
            "test",
            TestAvroSchema,
            GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
        private static readonly GlueSchemaRegistrySchema SpecificRecordSchema = new GlueSchemaRegistrySchema(
            "test",
            new User().Schema.ToString(),
            GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
        private static readonly byte[] Empty = Array.Empty<byte>();


        private static readonly AvroSerializer AvroSerializer = new AvroSerializer();

        private static User _userDefinedSpecificRecord;
        private static GenericRecord _userDefinedGenericRecord;
        
        private static GlueSchemaRegistryConfiguration _avroGenericConfiguration;
        private static GlueSchemaRegistryConfiguration _avroSpecificConfiguration;
        private static GlueSchemaRegistryConfiguration _avroUnknownConfiguration;



        [OneTimeSetUp]
        public static void Setup()
        {
            // Adding Console trace listener for debug outputs
            Trace.Listeners.Add(new ConsoleTraceListener());
            
            SetupAvroSpecificRecord();
            SetupAvroGenericRecord();
            SetupAvroDeserializers();
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            Trace.Flush();
        }

        private static void SetupAvroSpecificRecord()
        {
            var user = new User
            {
                Name = "stray",
                FavoriteNumber = 2022,
                FavoriteColor = "orange"
            };
            _userDefinedSpecificRecord = user;
        }

        private static void SetupAvroGenericRecord()
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            user.Add("name", "sansa");
            user.Add("favorite_number", 99);
            user.Add("favorite_color", "red");
            

            _userDefinedGenericRecord = user;
        }

        private static void SetupAvroDeserializers()
        {
            var genericDeserializerConfigs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord }
            };
            var specificDeserializerConfigs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.SpecificRecord }
            };
            var unknownDeserializerConfigs = new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.Unknown }
            };

            _avroGenericConfiguration = new GlueSchemaRegistryConfiguration(genericDeserializerConfigs);
            _avroSpecificConfiguration = new GlueSchemaRegistryConfiguration(specificDeserializerConfigs);
            _avroUnknownConfiguration = new GlueSchemaRegistryConfiguration(unknownDeserializerConfigs);
        }
        
        [Test]
        public void WhenDeserializeIsCalledReturnsCachedInstance()
        {
            var generic = AvroSerializer.Serialize(_userDefinedGenericRecord);
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);
            avroDeserializer.Deserialize(generic,
                GenericRecordSchema);

            Assert.AreEqual(1, avroDeserializer.CacheSize);
        }

        private static IEnumerable<TestCaseData> AvroDeserializerNullOrEmptyTestCases()
        {
            var avroSerializer = new AvroSerializer();
            
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            user.Add("name", "sansa");
            user.Add("favorite_number", 99);
            user.Add("favorite_color", "red");

            var serializedResult = avroSerializer.Serialize(user);
            
            
            yield return new TestCaseData(null, null);
            yield return new TestCaseData(null, GenericRecordSchema);
            yield return new TestCaseData(Empty, null);
            yield return new TestCaseData(Empty, GenericRecordSchema);
            yield return new TestCaseData(serializedResult, null);

        }

        [Test, TestCaseSource(nameof(AvroDeserializerNullOrEmptyTestCases))]
        public void TestDeserialize_NullOrEmpty_ThrowsException(byte[] data, GlueSchemaRegistrySchema schema)
        {
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);
            Assert.Throws(typeof(AwsSchemaRegistryException), () => avroDeserializer.Deserialize(data, schema));
            
            avroDeserializer = new AvroDeserializer(_avroSpecificConfiguration);
            Assert.Throws(typeof(AwsSchemaRegistryException), () => avroDeserializer.Deserialize(data, schema));
        }

        [Test]
        public void TestDeserialize_SpecificRecord_Succeed()
        {
            var serializedResult = AvroSerializer.Serialize(_userDefinedSpecificRecord);
            var avroDeserializer = new AvroDeserializer(_avroSpecificConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedResult,
                SpecificRecordSchema);
            Assert.True(deserialize is User);
    
            var deserializedUser = (User) deserialize;
            Assert.AreEqual(_userDefinedSpecificRecord.Name,deserializedUser.Name);
            Assert.AreEqual(_userDefinedSpecificRecord.FavoriteNumber,deserializedUser.FavoriteNumber);
            Assert.AreEqual(_userDefinedSpecificRecord.FavoriteColor,deserializedUser.FavoriteColor);
        
        }
        
        [Test]
        public void TestDeserialize_GenericRecord_Succeed()
        {
            var serializedResult = AvroSerializer.Serialize(_userDefinedGenericRecord);
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedResult,
                GenericRecordSchema);
            Assert.AreEqual(_userDefinedGenericRecord,deserialize);
        }
        
        
        [Test]
        public void TestDeserialize_GenericRecordIntoSpecificRecord_Succeed()
        {
            var serializedResult = AvroSerializer.Serialize(_userDefinedGenericRecord);
            
            var avroDeserializer = new AvroDeserializer(_avroSpecificConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedResult,
                GenericRecordSchema);
            
            Assert.True(deserialize is User);
    
            var deserializedUser = (User) deserialize;

            Assert.AreEqual(_userDefinedGenericRecord.GetValue(
                    _userDefinedGenericRecord.Schema["name"].Pos),
                deserializedUser.Name);
            Assert.AreEqual(_userDefinedGenericRecord.GetValue(
                    _userDefinedGenericRecord.Schema["favorite_number"].Pos),
                deserializedUser.FavoriteNumber);
            Assert.AreEqual(_userDefinedGenericRecord.GetValue(
                    _userDefinedGenericRecord.Schema["favorite_color"].Pos),
                deserializedUser.FavoriteColor);
        }
        
        [Test]
        public void TestDeserialize_SpecificRecordIntoGenericRecord_Succeed()
        {
            var serializedResult = AvroSerializer.Serialize(_userDefinedSpecificRecord);
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedResult,
                SpecificRecordSchema);
            Assert.True(deserialize is GenericRecord);
            var genericRecord = (GenericRecord) deserialize;

            Assert.AreEqual(_userDefinedSpecificRecord.Name, 
                genericRecord.GetValue(genericRecord.Schema["name"].Pos));
            Assert.AreEqual(_userDefinedSpecificRecord.FavoriteNumber, 
                genericRecord.GetValue(genericRecord.Schema["favorite_number"].Pos));
            Assert.AreEqual(_userDefinedSpecificRecord.FavoriteColor, 
                genericRecord.GetValue(genericRecord.Schema["favorite_color"].Pos));
        }
        
        [Test]
        public void TestDeserialize_EnumSchema_Succeed()
        {
            var enumSchema = AvroSchemas.GetValueOrDefault("user_enum.avsc", null);
            Assert.NotNull(enumSchema);
            var genericEnum = new GenericEnum((EnumSchema)enumSchema, "ONE");

            var serializedData = AvroSerializer.Serialize(genericEnum);
            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                enumSchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            
            Assert.AreEqual(genericEnum,deserialize);
        }
        
        [Test]
        public void TestDeserialize_IntegerArrays_Succeed()
        {
            // We do not have a easy way to serialize arrays due to a limitation in Avro library
            var item = 1;
            var arraySchema = AvroSchemas.GetValueOrDefault("user_array.avsc", null);
            Assert.NotNull(arraySchema);
            var intArray = new[]{item};
            var defaultWriter = new DefaultWriter(arraySchema);
            var memoryStream = new MemoryStream();
            Encoder encoder = new BinaryEncoder(memoryStream);
            defaultWriter.Write(intArray,encoder);
            var serializedData = memoryStream.ToArray();
            
            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                arraySchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            
            Assert.True(deserialize is Array);
            var deserializedArray = deserialize as Array;
            Assert.AreEqual(1, deserializedArray.Length);
            Assert.AreEqual(item, deserializedArray.GetValue(0));
        }
        
        [Test]
        public void TestDeserialize_ObjectArrays_Succeed()
        {
            // We do not have a easy way to serialize arrays due to a limitation in Avro library
            var item = 1;
            var arraySchema = AvroSchemas.GetValueOrDefault("user_array.avsc", null);
            Assert.NotNull(arraySchema);
            var intArray = new object[]{item};
            var defaultWriter = new DefaultWriter(arraySchema);
            var memoryStream = new MemoryStream();
            Encoder encoder = new BinaryEncoder(memoryStream);
            defaultWriter.Write(intArray,encoder);
            var serializedData = memoryStream.ToArray();
            
            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                arraySchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            
            Assert.True(deserialize is Array);
            var deserializedArray = deserialize as Array;
            Assert.AreEqual(1, deserializedArray.Length);
            Assert.AreEqual(item, deserializedArray.GetValue(0));
        }
        
        [Test]
        public void TestDeserialize_StringArrays_Succeed()
        {
            // We do not have a easy way to serialize arrays due to a limitation in Avro library
            var item = "TestValue";
            var arraySchema = AvroSchemas.GetValueOrDefault("user_array_String.avsc", null);
            Assert.NotNull(arraySchema);
            var intArray = new[]{item};
            var defaultWriter = new DefaultWriter(arraySchema);
            var memoryStream = new MemoryStream();
            Encoder encoder = new BinaryEncoder(memoryStream);
            defaultWriter.Write(intArray,encoder);
            var serializedData = memoryStream.ToArray();
            
            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                arraySchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            
            Assert.True(deserialize is Array);
            var deserializedArray = deserialize as Array;
            Assert.AreEqual(1, deserializedArray.Length);
            Assert.AreEqual(item, deserializedArray.GetValue(0));
        }
        
        [Test]
        public void TestDeserialize_Unions_Succeed()
        {
            var unionSchema = AvroSchemas.GetValueOrDefault("user_union.avsc", null);
            Assert.NotNull(unionSchema);
            var unionRecord = new GenericRecord((RecordSchema)unionSchema);
            unionRecord.Add("experience", 1);
            unionRecord.Add("age", 30);

            var serializedData = AvroSerializer.Serialize(unionRecord);

            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                unionSchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            
            Assert.AreEqual(unionRecord,deserialize);
        }
        
        [Test]
        public void TestDeserialize_UnionsWithNull_Succeed()
        {
            var unionSchema = AvroSchemas.GetValueOrDefault("user_union.avsc", null);
            Assert.NotNull(unionSchema);
            var unionRecord = new GenericRecord((RecordSchema)unionSchema);
            unionRecord.Add("experience", null);
            unionRecord.Add("age", 30);

            var serializedData = AvroSerializer.Serialize(unionRecord);

            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                unionSchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            
            Assert.AreEqual(unionRecord,deserialize);
        }
        
        [Test]
        public void TestDeserialize_FixedArray_Succeed()
        {
            var fixedSchema = AvroSchemas.GetValueOrDefault("user_fixed.avsc", null);
            Assert.NotNull(fixedSchema);
            var fixedRecord = new GenericFixed((FixedSchema)fixedSchema);
            var bytes = Encoding.ASCII.GetBytes("byte array");
            fixedRecord.Value = bytes;


            var serializedData = AvroSerializer.Serialize(fixedRecord);

            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                fixedSchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            
            Assert.AreEqual(fixedRecord,deserialize);
        }
        
        [Test]
        public void TestDeserialize_Maps_Succeed()
        {
            const string avroRecordMapName = "meta";
            const string keyName = "testKey";
            
            var mapSchema = AvroSchemas.GetValueOrDefault("user_map.avsc", null);
            Assert.NotNull(mapSchema);
            var mapRecord = new GenericRecord((RecordSchema)mapSchema);
            var dictionary = new Dictionary<string, object> { { keyName, 1L } };
            mapRecord.Add(avroRecordMapName, dictionary);

            var serializedData = AvroSerializer.Serialize(mapRecord);
            
            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                mapSchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);
            
            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            Assert.True(deserialize is GenericRecord);
            var record = (GenericRecord) deserialize;
            var deserializedMap = (Dictionary<string,object>)record.GetValue(record.Schema[avroRecordMapName].Pos);
            Assert.AreEqual(dictionary.Keys.GetEnumerator().Current, deserializedMap.Keys.GetEnumerator().Current);
            Assert.AreEqual(dictionary[keyName], deserializedMap[keyName]);
        }
        
        [Test]
        public void TestDeserialize_AllTypes_Succeed()
        {
            const string avroRecordMapName = "meta";
            const string keyName = "testKey";
            
            var mixedSchema = (RecordSchema) AvroSchemas.GetValueOrDefault("user3.avsc", null);
            Assert.NotNull(mixedSchema);
            var mixedRecord = new GenericRecord(mixedSchema);
            
            var dictionary = new Dictionary<string, object> { { keyName, 1L } };
            var intList = new[] { 1 };

            var enumSchema = mixedSchema["integerEnum"].Schema;
            var enumSymbol = new GenericEnum((EnumSchema)enumSchema, "ONE");
            
            
            mixedRecord.Add(avroRecordMapName, dictionary);
            mixedRecord.Add("name", "Joe");
            mixedRecord.Add("favorite_number", 1);
            mixedRecord.Add(avroRecordMapName, dictionary);
            mixedRecord.Add("listOfColours", intList);
            mixedRecord.Add("integerEnum", enumSymbol);
            
            
            var serializedData = AvroSerializer.Serialize(mixedRecord);
            
            var schemaObject = new GlueSchemaRegistrySchema("testAvroSchema", 
                mixedSchema.ToString(), 
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());
            
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);
            
            var deserialize = avroDeserializer.Deserialize(serializedData,
                schemaObject);
            Assert.True(deserialize is GenericRecord);
            var record = (GenericRecord) deserialize;
            var deserializedMap = (Dictionary<string,object>)record.GetValue(record.Schema[avroRecordMapName].Pos);
            Assert.AreEqual(dictionary.Keys.GetEnumerator().Current, 
                deserializedMap.Keys.GetEnumerator().Current);
            Assert.AreEqual(dictionary[keyName], deserializedMap[keyName]);
            Assert.AreEqual("Joe", record.GetValue(record.Schema["name"].Pos));
            Assert.AreEqual(1, record.GetValue(record.Schema["favorite_number"].Pos));
            Assert.AreEqual(intList, record.GetValue(record.Schema["listOfColours"].Pos));
            Assert.AreEqual(enumSymbol, record.GetValue(record.Schema["integerEnum"].Pos));
        }
        
        /*
         * Test Invalid record types 
         */
        
        [Test]
        public void TestDeserialize_UnknownRecordType_ThrowsException()
        {
            var serializedResult = AvroSerializer.Serialize(_userDefinedGenericRecord);
            var avroDeserializer = new AvroDeserializer(_avroUnknownConfiguration);

            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                code: () => avroDeserializer.Deserialize(serializedResult,GenericRecordSchema));
            Assert.AreEqual("Unsupported AvroRecordType: Unknown", exception.GetBaseException().Message);
        }
        
        [Test]
        public void TestDeserialize_InvalidSchema_ThrowsException()
        {
            var serializedResult = AvroSerializer.Serialize(_userDefinedGenericRecord);
            var avroDeserializer = new AvroDeserializer(_avroUnknownConfiguration);

            var invalid = new GlueSchemaRegistrySchema(
                "invalid",
                "invalid",
                GlueSchemaRegistryConstants.DataFormat.AVRO.ToString());

            Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => avroDeserializer.Deserialize(serializedResult, invalid));
        }
        
        [Test]
        public void TestDeserialize_NullData_ThrowsException()
        {
            var avroDeserializer = new AvroDeserializer(_avroGenericConfiguration);

            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => avroDeserializer.Deserialize(null, GenericRecordSchema));
            Assert.AreEqual(typeof(ArgumentNullException), exception.GetBaseException().GetType());
        }
        
        [Test]
        public void TestDeserialize_NullSchema_ThrowsException()
        {
            var serializedResult = AvroSerializer.Serialize(_userDefinedGenericRecord);
            var avroDeserializer = new AvroDeserializer(_avroUnknownConfiguration);


            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => avroDeserializer.Deserialize(serializedResult, null));
            Assert.AreEqual(typeof(NullReferenceException), exception.GetBaseException().GetType());
        }
    }
}
