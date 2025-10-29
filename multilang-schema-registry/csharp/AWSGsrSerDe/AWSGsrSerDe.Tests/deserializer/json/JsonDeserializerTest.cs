using System;
using System.Text;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer.json;
using AWSGsrSerDe.serializer.json;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.deserializer.json
{
    [TestFixture]
    public class JsonDeserializerTest
    {
        private JsonDeserializer _jsonDeserializer = new JsonDeserializer();

        [Test]
        public void testDeserialize_nullArgs_throwsException()
        {
            string testSchemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                                          + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                                          + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                                          + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                                          + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                                          + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                                          + "\"maximum\":180}},\"additionalProperties\":false}";
            string jsonData = "{\"latitude\":48.858093,\"longitude\":2.294694}";

            var testBytes = Encoding.UTF8.GetBytes(jsonData);
            var testSchema = new GlueSchemaRegistrySchema("testJson", testSchemaDefinition,
                GlueSchemaRegistryConstants.DataFormat.JSON.ToString());

            Assert.Throws(typeof(ArgumentNullException), () => { _jsonDeserializer.Deserialize(null, testSchema); });
            Assert.Throws(typeof(ArgumentNullException), () => { _jsonDeserializer.Deserialize(testBytes, null); });
        }
        
        [Test]
        public void testDeserialize_succeeds()
        {
            string testSchemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                                          + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                                          + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                                          + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                                          + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                                          + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                                          + "\"maximum\":180}},\"additionalProperties\":false}";
            string jsonData = "{\"latitude\":48.858093,\"longitude\":2.294694}";

            var testBytes = Encoding.UTF8.GetBytes(jsonData);
            var testSchema = new GlueSchemaRegistrySchema("testJson", testSchemaDefinition,
                GlueSchemaRegistryConstants.DataFormat.JSON.ToString());

            var deserializedObject = _jsonDeserializer.Deserialize(testBytes, testSchema);
            Assert.IsTrue(deserializedObject is JsonDataWithSchema);
            
            var jsonDataWithSchema = (JsonDataWithSchema) deserializedObject;
            Assert.AreEqual(jsonData, jsonDataWithSchema.Payload);
            Assert.AreEqual(testSchemaDefinition, jsonDataWithSchema.Schema);
        }
    }
}
