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
using System.Text.Json.Nodes;
using AWSGsrSerDe.common;
using AWSGsrSerDe.serializer.json;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace AWSGsrSerDe.deserializer.json
{
    /// <summary>
    /// Json specific de-serializer responsible for handling the Json data format
    /// specific deserialization behavior.
    /// </summary>
    public class JsonDeserializer : IDataFormatDeserializer
    {
        private readonly Type _objectType;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDeserializer"/> class.
        /// </summary>
        public JsonDeserializer()
        {
            _objectType = null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDeserializer"/> class.
        /// </summary>
        /// <param name="config">configuration elements</param>
        /// <exception cref="ArgumentNullException">config passed in is null.</exception>
        public JsonDeserializer([NotNull] GlueSchemaRegistryDataFormatConfiguration config)
        {
            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            // There is no easy way to inject classname as a parameter for C# class during serialization
            // therefore we will not de-serialize json bytes to object if corresponding type is not provided
            _objectType = config.JsonObjectType;
        }

        /// <inheritdoc />
        public object Deserialize([NotNull] byte[] data, [NotNull] GlueSchemaRegistrySchema schema)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (schema is null)
            {
                throw new ArgumentNullException(nameof(schema));
            }

            var schemaDefinition = schema.SchemaDef;

            var schemaNode = JsonNode.Parse(schemaDefinition);
            object deserializedObject;
            if (_objectType is null)
            {
                var dataNode = JsonNode.Parse(data);
                if (dataNode is null)
                {
                    throw new AwsSchemaRegistryException("Cannot parse byte array into Json");
                }

                if (schemaNode is null)
                {
                    throw new AwsSchemaRegistryException("Schema definition cannot be parsed");
                }

                deserializedObject = JsonDataWithSchema.Build(
                    schemaNode.ToJsonString(),
                    dataNode.ToJsonString());
            }
            else
            {
                var readOnlySpan = new ReadOnlySpan<byte>(data);
                deserializedObject = JsonSerializer.Deserialize(readOnlySpan, _objectType);
            }

            return deserializedObject;
        }
    }
}
