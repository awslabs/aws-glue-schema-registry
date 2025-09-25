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
        
        // Evolution test protobuf messages using compiled proto classes
        public static Google.Protobuf.IMessage CreateUserV1Proto()
        {
            // Generated from: /shared/test/protos/evolution/positive/backward/UserV1.proto
            var user = new Evolution.Test.UserV1();
            user.Name = "John Doe";
            user.Age = 30;
            user.Email = "john@example.com";
            return user;
        }

        public static Google.Protobuf.IMessage CreateUserV2Proto()
        {
            // Generated from: /shared/test/protos/evolution/positive/backward/UserV2.proto
            var user = new Evolution.Test.UserV2();
            user.Name = "Jane Doe";
            user.Age = 25;
            user.Email = "jane@example.com";
            user.Phone = "555-1234";
            user.Address = "123 Main St";
            return user;
        }

        public static Google.Protobuf.IMessage CreateUserV3Proto()
        {
            // Generated from: /shared/test/protos/evolution/positive/backward/UserV3.proto
            var user = new Evolution.Test.UserV3();
            user.Name = "Bob Smith";
            user.Age = 35;
            user.Phone = "555-5678";
            user.Address = "456 Oak Ave";
            user.Department = "Engineering";
            return user;
        }

        public static Google.Protobuf.IMessage CreateUserV1IncompatibleProto()
        {
            // Generated from: /shared/test/protos/evolution/negative/backward/UserV1Incompatible.proto
            var user = new Evolution.Test.UserV1Incompatible();
            user.Name = "Jane Smith";
            user.Age = 28;
            user.Email = 12345; // This will cause compilation error - incompatible type
            return user;
        }
    }
}