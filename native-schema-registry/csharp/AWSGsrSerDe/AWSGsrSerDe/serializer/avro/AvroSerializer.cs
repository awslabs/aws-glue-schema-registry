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

using System.IO;
using Avro;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using AWSGsrSerDe.common;
using Microsoft.Extensions.Caching.Memory;

namespace AWSGsrSerDe.serializer.avro
{
    /// <summary>
    /// Avro serialization helper.
    /// </summary>
    public class AvroSerializer : IDataFormatSerializer
    {
        private readonly MemoryCache _datumWriterCache;

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroSerializer"/> class.
        /// </summary>
        /// <param name="configuration">configuration elements</param>
        public AvroSerializer(GlueSchemaRegistryConfiguration configuration = null)
            : this()
        {
        }

        private AvroSerializer()
        {
            // TODO: make cache size limit configurable
            _datumWriterCache = new MemoryCache(new MemoryCacheOptions { SizeLimit = 1000 });
        }

        /// <summary>
        /// Gets size of datum writer cache for testing purpose
        /// </summary>
        public int CacheSize => _datumWriterCache.Count;

        /// <summary>
        /// Serialize the Avro message to bytes
        /// </summary>
        /// <param name="data">the Avro message for serialization</param>
        /// <returns>the serialized byte array</returns>
        public byte[] Serialize(object data)
        {
            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (data == null)
            {
                return null;
            }

            return Serialize(data, GetOrCreateDatumWriter(data));
        }

        /// <summary>
        /// Get the schema definition.
        /// </summary>
        /// <param name="data">object for which schema definition has to be derived</param>
        /// <returns>schema string</returns>
        public string GetSchemaDefinition(object data)
        {
            var schema = GetSchema(data);
            return schema.ToString();
        }

        /// <inheritdoc />
        public void Validate(string schemaDefinition, byte[] data)
        {
            // No-op
            // We cannot determine accurately if the data bytes match the schema as Avro bytes don't contain the field names.
        }

        /// <inheritdoc />
        public void Validate(object data)
        {
            // No-op
            // Avro format assumes that the passed object contains schema and data that are mutually conformant.
            // We cannot validate the data against the schema.
        }

        /// <inheritdoc />
        public void SetAdditionalSchemaInfo(object data, ref GlueSchemaRegistrySchema schema)
        {
            // No-op
            // Currently we do not need to modify schema object for Avro message
        }

        private DatumWriter<object> GetOrCreateDatumWriter(object data)
        {
            var schema = GetSchema(data);
            var dataType = data.GetType().ToString();
            var cacheKey = schema + dataType;
            var datumWriter = _datumWriterCache.GetOrCreate(
                cacheKey,
                entry =>
                {
                    entry.Size = 1;
                    return NewDatumWriter(data, schema);
                });
            return datumWriter;
        }

        private static byte[] Serialize(object data, DatumWriter<object> datumWriter)
        {
            var memoryStream = new MemoryStream();
            Encoder encoder = new BinaryEncoder(memoryStream);
            datumWriter.Write(data, encoder);
            var result = memoryStream.ToArray();
            memoryStream.Close();
            return result;
        }

        private static Schema GetSchema(object data)
        {
            return data switch
            {
                GenericRecord record => record.Schema,
                GenericEnum record => record.Schema,
                ISpecificRecord record => record.Schema,
                GenericFixed record => record.Schema,
                _ => throw new AwsSchemaRegistryException("Unsupported Avro Data formats"),
            };
        }

        private static DatumWriter<object> NewDatumWriter(object data, Schema schema)
        {
            return data switch
            {
                ISpecificRecord _ => new SpecificDatumWriter<object>(schema),
                GenericRecord _ => new GenericWriter<object>(schema),
                GenericEnum _ => new GenericWriter<object>(schema),
                GenericFixed _ => new GenericWriter<object>(schema),
                _ => throw new AwsSchemaRegistryException($"Unsupported type passed for serialization: {data}"),
            };
        }
    }
}
