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
using System.Text;
using AWSGsrSerDe.serializer.json;
using AWSGsrSerDe.Tests.utils;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.serializer.json
{
    [TestFixture]
    public class JsonSerializerTest
    {
        private static Car SPECIFIC_TEST_RECORD = new Car
        {
            make = "Honda",
            model = "crv",
            used = true,
            miles = 10000,
            listedDate = DateTime.Now,
            purchaseDate = DateTime.Parse("2000-01-01T00:00:00.000Z"),
            owners = new[] { "John", "Jane", "Hu" },
            serviceCheckes = new[] { 5000.0f, 10780.30f },
        };

        private static SchemaLoader.JsonGenericRecord GENERIC_TEST_RECORD = SchemaLoader.LoadJsonGenericRecord(
            "schema/draft07/geographical-location.schema.json",
            "geolocation1.json",
            true);

        private readonly JsonSerializer _jsonSerializer = new JsonSerializer();

        [Test]
        public void testWrapper_serializeWithGenericRecord_bytesMatch()
        {
            var record = RecordGenerator.GetSampleJsonTestData();
            var payload = "{\"latitude\":48.858093,\"longitude\":2.294694}";

            var expectedBytes = Encoding.UTF8.GetBytes(payload);
            var serializedBytes = _jsonSerializer.Serialize(record);

            Assert.AreEqual(expectedBytes, serializedBytes);
        }

        [Test]
        public void testWrapper_serializeWithJsonString_bytesMatch()
        {
            var payload = "{\"latitude\":48.858093,\"longitude\":2.294694}";

            var expectedBytes = Encoding.UTF8.GetBytes(payload);
            var serializedBytes = _jsonSerializer.Serialize(payload);

            Assert.AreEqual(expectedBytes, serializedBytes);
        }

        // TODO: Fix this test
        // [Test]
        // public void testWrapper_serializeWithSpecificRecord_bytesMatch()
        // {
        //     var expectedBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(SPECIFIC_TEST_RECORD);
        //     var serializedBytes = _jsonSerializer.Serialize(SPECIFIC_TEST_RECORD);

        //     Assert.AreEqual(expectedBytes, serializedBytes);
        // }

        [Test]
        public void testValidate_validatesWrapper_successfully()
        {
            var jsonDataWithSchema = JsonDataWithSchema.Build(GENERIC_TEST_RECORD.Schema, GENERIC_TEST_RECORD.Payload);
            Assert.DoesNotThrow(() => { _jsonSerializer.Serialize(jsonDataWithSchema); });
        }

        [Test]
        public void testValidate_validatesSpecificRecord_successfully()
        {
            Assert.DoesNotThrow(() => { _jsonSerializer.Serialize(SPECIFIC_TEST_RECORD); });
        }

        [Test]
        public void testValidate_validatesWrapper_ThrowsValidationException()
        {
            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => { _jsonSerializer.Serialize(RecordGenerator.CreateNonSchemaConformantJsonData()); });
            Assert.IsTrue(exception.Message.StartsWith("Validation failed with"));
        }

        [Test]
        public void testValidate_validatesBytes_successfully()
        {
            var dataBytes = Encoding.Default.GetBytes(GENERIC_TEST_RECORD.Payload);
            var schemaDefinition = GENERIC_TEST_RECORD.Schema;
            Assert.DoesNotThrow(() => { _jsonSerializer.Validate(schemaDefinition, dataBytes); });
        }

        [Test]
        public void testValidate_validatesBytes_ThrowsValidationException()
        {
            var nonSchemaConformantJsonData = RecordGenerator.CreateNonSchemaConformantJsonData();
            var dataBytes = Encoding.Default.GetBytes(nonSchemaConformantJsonData.Payload);
            var schemaDefinition = nonSchemaConformantJsonData.Schema;

            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => { _jsonSerializer.Validate(schemaDefinition, dataBytes); });
            Assert.IsTrue(exception.Message.StartsWith("Validation failed with"));
        }

        [Test]
        public void testValidate_validatesBytes_NonUTF8EncodingThrowsException()
        {
            //Encoding as UTF-32 Bytes
            var dataBytes = Encoding.UTF32.GetBytes(GENERIC_TEST_RECORD.Payload);
            var schemaDefinition = GENERIC_TEST_RECORD.Schema;

            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => { _jsonSerializer.Validate(schemaDefinition, dataBytes); });
            Assert.AreEqual("Malformed JSON", exception.Message);
        }

        [Test]
        public void testWrapper_getSchemaDefinition_matches()
        {
            var schemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                                   + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                                   + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                                   + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                                   + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                                   + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                                   + "\"maximum\":180}},\"additionalProperties\":false}";
            var jsonDataWithSchema = JsonDataWithSchema.Build(GENERIC_TEST_RECORD.Schema, GENERIC_TEST_RECORD.Payload);
            Assert.AreEqual(schemaDefinition, _jsonSerializer.GetSchemaDefinition(jsonDataWithSchema));
        }

        [Test]
        public void testObject_getSchemaDefinition_matches()
        {
            var schemaDefinition = "{\"$schema\":\"http://json-schema.org/draft-04/schema#\","
                                   + "\"title\":\"Car\",\"type\":\"object\","
                                   + "\"description\":\"This is a car\",\"additionalProperties\":false,"
                                   + "\"required\":[\"make\",\"model\"],"
                                   + "\"properties\":{\"make\":{\"type\":\"string\",\"minLength\":1},"
                                   + "\"model\":{\"type\":\"string\",\"minLength\":1},"
                                   + "\"used\":{\"type\":\"boolean\",\"default\":true},"
                                   + "\"miles\":{\"type\":\"integer\",\"maximum\":200000.0,\"minimum\":0.0},"
                                   + "\"purchaseDate\":{\"type\":\"string\",\"format\":\"date-time\"},"
                                   + "\"listedDate\":{\"type\":\"string\",\"format\":\"date-time\"},"
                                   + "\"owners\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
                                   + "\"serviceCheckes\":{\"type\":\"array\",\"items\":{\"type\":\"number\",\"format\":\"float\"}}}}";
            Assert.AreEqual(schemaDefinition, _jsonSerializer.GetSchemaDefinition(SPECIFIC_TEST_RECORD));
        }

        [Test]
        public void testGetSchemaDefinition_nullObject_throwsException()
        {
            Assert.Throws(
                typeof(ArgumentNullException),
                () => { _jsonSerializer.GetSchemaDefinition(null); });
        }

        [Test]
        public void testSerialize_nullObject_throwsException()
        {
            Assert.Throws(
                typeof(ArgumentNullException),
                () => { _jsonSerializer.Serialize(null); });
        }
    }
}
