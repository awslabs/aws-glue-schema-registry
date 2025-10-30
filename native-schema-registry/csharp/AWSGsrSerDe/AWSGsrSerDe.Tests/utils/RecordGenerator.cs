using Avro;
using Avro.Generic;
using AWSGsrSerDe.serializer.json;
using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

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

        // New fuzzing methods - appended as requested
        public static GenericRecord GetFuzzedAvroRecord(byte[] fuzzData, Random rand)
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            user.Add("name", FuzzStringValue("Alyssa", fuzzData, rand));
            user.Add("favorite_number", rand.Next(-1000000, 1000000));
            user.Add("favorite_color", rand.Next(0, 3) == 0 ? null : FuzzStringValue("blue", fuzzData, rand));
            return user;
        }

        public static JsonDataWithSchema GetFuzzedJsonTestData(byte[] fuzzData, Random rand)
        {
            var baseData = GetSampleJsonTestData();
            var jsonObj = JsonNode.Parse(baseData.Payload);
            
            FuzzJsonNodeRecursive(jsonObj, fuzzData, rand, 0);
            
            return JsonDataWithSchema.Build(baseData.Schema, jsonObj.ToJsonString());
        }

        private static void FuzzJsonNodeRecursive(JsonNode node, byte[] data, Random rand, int depth)
        {
            if (depth > 3 || node == null) return;
            
            if (node is JsonObject obj)
            {
                var keys = obj.Select(kv => kv.Key).ToList();
                foreach (var key in keys)
                {
                    var value = obj[key];
                    if (value?.GetValueKind() == JsonValueKind.String)
                        obj[key] = FuzzStringValue(value.GetValue<string>(), data, rand);
                    else if (value?.GetValueKind() == JsonValueKind.Number)
                        obj[key] = rand.NextDouble() * 1000;
                    else
                        FuzzJsonNodeRecursive(value, data, rand, depth + 1);
                }
            }
        }

        private static string FuzzStringValue(string baseStr, byte[] data, Random rand)
        {
            if (data.Length < 4) return baseStr;
            
            var fuzzType = rand.Next(0, 6);
            switch (fuzzType)
            {
                case 0: return new string((char)rand.Next(32, 127), rand.Next(1, 50));
                case 1: return baseStr + Encoding.UTF8.GetString(data.Take(rand.Next(1, Math.Min(20, data.Length))).ToArray());
                case 2: return string.Empty;
                case 3: return new string('X', rand.Next(1, 100));
                case 4: return Convert.ToBase64String(data.Take(rand.Next(1, Math.Min(30, data.Length))).ToArray());
                default: return baseStr;
            }
        }
    }
}
