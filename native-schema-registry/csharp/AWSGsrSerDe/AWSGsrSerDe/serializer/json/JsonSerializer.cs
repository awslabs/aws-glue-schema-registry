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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using NJsonSchema;

namespace AWSGsrSerDe.serializer.json
{
    /// <summary>
    /// Json serialization helper.
    /// </summary>
    public class JsonSerializer : IDataFormatSerializer
    {
        /// <inheritdoc />
        public byte[] Serialize([NotNull]object data)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data), "Cannot take null value when serialize");
            }

            var dataNode = GetDataNode(data);
            var jsonSchema = GetSchema(data);

            // JsonValidator.ValidateDataWithSchema(jsonSchema, dataNode);

            return WriteBytes(dataNode);
        }

        /// <inheritdoc />
        public string GetSchemaDefinition([NotNull]object data)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data), "Cannot get schema definition of null");
            }

            var schemaNode = GetSchemaNode(data);
            return schemaNode.ToJsonString();
        }


        /// <inheritdoc />
        public void Validate(string schemaDefinition, byte[] data)
        {
            // No-op
            // JSON validation has been disabled for performance reasons.
            // The Java validation layer still provides schema compliance checking.
        }

        /// <inheritdoc />
        public void Validate(object data)
        {
            // No-op
            // JSON validation has been disabled for performance reasons.
            // The Java validation layer still provides schema compliance checking.
        }

        /// <inheritdoc />
        public void SetAdditionalSchemaInfo(object data, ref GlueSchemaRegistrySchema schema)
        {
            // No-op
            // Currently we do not need to modify schema object for Json message
        }

        private static bool IsWrapper(object data)
        {
            return data is JsonDataWithSchema;
        }

        private static byte[] WriteBytes(JsonNode data)
        {
            var options = new JsonWriterOptions();
            using var stream = new MemoryStream();
            using var writer = new Utf8JsonWriter(stream, options);

            data.WriteTo(writer, new JsonSerializerOptions());
            writer.Flush();
            var array = stream.ToArray();
            stream.Close();
            return array;
        }

        private static JsonSchema GetSchema(object data)
        {
            try
            {
                if (IsWrapper(data))
                {
                    var dataObject = (JsonDataWithSchema)data;
                    var getSchemaTask = JsonSchema.FromJsonAsync(dataObject.Schema);
                    return getSchemaTask.Result;
                }

                var schema = data is string dataJsonString
                    ? JsonSchema.FromSampleJson(dataJsonString)
                    : JsonSchema.FromType(data.GetType());
                return schema;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException(
                    "Could not generate schema from the type provided " + data.GetType(), e);
            }
        }

        private static JsonNode GetSchemaNode([NotNull] object data)
        {
            if (IsWrapper(data))
            {
                var dataObject = (JsonDataWithSchema)data;
                return ConvertToJsonNode(dataObject.Schema);
            }

            try
            {
                var schema = data is string dataJsonString
                    ? JsonSchema.FromSampleJson(dataJsonString)
                    : JsonSchema.FromType(data.GetType());
                var schemaNode = JsonNode.Parse(schema.ToJson());
                return schemaNode;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException(
                    "Could not generate schema from the type provided " + data.GetType(), e);
            }
        }

        private static JsonNode GetDataNode([NotNull] object data)
        {
            if (IsWrapper(data))
            {
                var dataObject = (JsonDataWithSchema)data;
                return ConvertToJsonNode(dataObject.Payload);
            }

            return GetDataNodeFromSpecificObject(data);
        }

        private static JsonNode GetDataNodeFromSpecificObject(object data)
        {
            try
            {
                var jsonString = data as string ?? System.Text.Json.JsonSerializer.Serialize(data);
                return JsonNode.Parse(jsonString);
            }
            catch (NotSupportedException e)
            {
                throw new AwsSchemaRegistryException("Not a valid Specific Json Record.", e);
            }
        }

        private static JsonNode ConvertToJsonNode(string jsonString)
        {
            try
            {
                return JsonNode.Parse(jsonString);
            }
            catch (JsonException e)
            {
                throw new AwsSchemaRegistryException("Malformed JSON", e);
            }
        }
    }
}
