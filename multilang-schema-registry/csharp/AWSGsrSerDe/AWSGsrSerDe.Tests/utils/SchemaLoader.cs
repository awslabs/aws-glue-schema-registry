using System;
using System.Collections.Generic;
using System.IO;
using Avro;

namespace AWSGsrSerDe.Tests.utils
{
    public static class SchemaLoader
    {
        private const string RelativePathPrefix = "../../../../../../../serializer-deserializer/src/test/resources/";

        public static Schema LoadAvroSchema(string schemaFilePath)
        {
            Schema schema = null;
            try
            {
                var schemaDefinition = File.ReadAllText(schemaFilePath);
                schema = Schema.Parse(schemaDefinition);
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException("Failed to parse the avro schema file", e);
            }

            return schema;
        }

        public static Dictionary<string, Schema> LoadAllAvroSchemas()
        {
            var avroSchemas = new Dictionary<string, Schema>();
            const string directoryPath = RelativePathPrefix + "avro/";
            try
            {
                var filePaths = Directory.GetFiles(directoryPath);
                foreach (var filePath in filePaths)
                {
                    var fileName = Path.GetFileName(filePath);
                    var schema = LoadAvroSchema(filePath);
                    avroSchemas.Add(fileName, schema);
                }

                return avroSchemas;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException("Failed to load avro schema files in given directory ", e);
            }
        }

        public static JsonGenericRecord LoadJsonGenericRecord(string schemaRelativePath, string payloadRelativePath,
            bool isValid)
        {
            const string jsonResourcePath = RelativePathPrefix + "json/";
            var schema = File.ReadAllText(jsonResourcePath + schemaRelativePath);
            var payload = File.ReadAllText(jsonResourcePath + payloadRelativePath);

            return new JsonGenericRecord { Schema = schema, Payload = payload, IsValid = isValid };
        }

        public class JsonGenericRecord
        {
            public string Schema;
            public string Payload;
            public bool IsValid;
        }
    }
}
