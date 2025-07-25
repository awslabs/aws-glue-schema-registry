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
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Avro;
using AWSGsrSerDe.common;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace AWSGsrSerDe.deserializer.protobuf
{
    /// <inheritdoc />
    public class ProtobufDeserializer : IDataFormatDeserializer
    {
        private readonly MessageDescriptor _descriptor;
        private static readonly Dictionary<string, MessageDescriptor> _descriptorCache = new Dictionary<string, MessageDescriptor>();

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtobufDeserializer"/> class.
        /// </summary>
        /// <param name="config">configuration element</param>
        public ProtobufDeserializer([NotNull] GlueSchemaRegistryConfiguration config)
        {
            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            // For Kafka deserialization, the descriptor can be null and will be resolved dynamically from the schema
            _descriptor = config.ProtobufMessageDescriptor;
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

            try
            {
                MessageDescriptor descriptor;
                if (_descriptor != null)
                {
                    // Use pre-configured descriptor
                    descriptor = _descriptor;
                }
                else
                {
                    // Resolve descriptor dynamically from the schema
                    descriptor = GetMessageDescriptorFromSchema(schema);
                }

                var message = descriptor.Parser.ParseFrom(data);
                return message;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException("Exception occurred while de-serializing Protobuf message", e);
            }
        }

        /// <summary>
        /// Resolves the MessageDescriptor from the GlueSchemaRegistrySchema
        /// </summary>
        /// <param name="schema">The schema containing the protobuf descriptor information</param>
        /// <returns>The resolved MessageDescriptor</returns>
        private static MessageDescriptor GetMessageDescriptorFromSchema(GlueSchemaRegistrySchema schema)
        {
            var cacheKey = $"{schema.SchemaName}:{schema.AdditionalSchemaInfo}";
            
            // Check cache first
            if (_descriptorCache.TryGetValue(cacheKey, out var cachedDescriptor))
            {
                return cachedDescriptor;
            }

            try
            {
                // Decode the base64 schema definition to get file descriptor bytes
                var schemaBytes = Convert.FromBase64String(schema.SchemaDef);
                
                // Parse the file descriptor
                var fileDescriptor = FileDescriptor.Parser.ParseFrom(schemaBytes);
                
                // Find the specific message descriptor using the full name from AdditionalSchemaInfo
                var messageFullName = schema.AdditionalSchemaInfo;
                if (string.IsNullOrEmpty(messageFullName))
                {
                    throw new AwsSchemaRegistryException("AdditionalSchemaInfo is required for protobuf deserialization but is null or empty");
                }

                var messageDescriptor = fileDescriptor.MessageTypes.FirstOrDefault(msg => msg.FullName == messageFullName);
                if (messageDescriptor == null)
                {
                    throw new AwsSchemaRegistryException($"Message descriptor with full name '{messageFullName}' not found in the schema");
                }

                // Cache the descriptor for future use
                _descriptorCache[cacheKey] = messageDescriptor;
                
                return messageDescriptor;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException($"Failed to resolve MessageDescriptor from schema: {e.Message}", e);
            }
        }
    }
}
