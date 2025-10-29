using Avro;
using Avro.Generic;
using AWSGsrSerDe.serializer.json;

namespace AWSGsrSerDe.Tests.utils
{
    public class RecordGenerator
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
        
        public static JsonDataWithSchema CreateNonSchemaConformantJsonData()
        {
            var invalidJsonGenericRecord = SchemaLoader.LoadJsonGenericRecord(
                "schema/draft07/person.schema.json",
                "produce1.json", false);
            return CreateGenericJsonRecord(invalidJsonGenericRecord);
        }

        public static JsonDataWithSchema GetSampleJsonTestData()
        {
            var loadJsonGenericRecord = SchemaLoader.LoadJsonGenericRecord(
                "schema/draft07/geographical-location.schema.json",
                "geolocation1.json",
                true);
            return CreateGenericJsonRecord(loadJsonGenericRecord);
        }

        public static JsonDataWithSchema CreateGenericJsonRecord(SchemaLoader.JsonGenericRecord jsonGenericRecord)
        {
            return JsonDataWithSchema.Build(jsonGenericRecord.Schema, jsonGenericRecord.Payload);
        }
        
        public static GenericRecord GetTestAvroRecord()
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            user.Add("name", "Alyssa🌯 🫔 🥗 🥘 🫕 🥫 🍝 🍜 🍲 🍛 🍣 🍱 🥟 🦪 🍤 🍙 🍚 🍘 🍥");
            user.Add("favorite_number", 256);
            user.Add("favorite_color", "blue");
            return user;
        }
    }
}