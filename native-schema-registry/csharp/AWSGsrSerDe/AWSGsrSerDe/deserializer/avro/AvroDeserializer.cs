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
using Avro;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using AWSGsrSerDe.common;
using Microsoft.Extensions.Caching.Memory;

namespace AWSGsrSerDe.deserializer.avro
{
    /// <summary>
    /// Avro specific de-serializer responsible for handling the Avro protocol
    /// specific conversion behavior.
    /// </summary>
    public class AvroDeserializer : IDataFormatDeserializer
    {
        private readonly MemoryCache _datumReaderCache;
        private AvroRecordType _avroRecordType;

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDeserializer"/> class.
        /// </summary>
        /// <param name="configs">configuration elements</param>
        public AvroDeserializer(GlueSchemaRegistryConfiguration configs)
            : this()
        {
            _avroRecordType = configs.AvroRecordType;
        }

        private AvroDeserializer()
        {
            // TODO: make the cache size limit configurable
            _datumReaderCache = new MemoryCache(new MemoryCacheOptions { SizeLimit = 1000 });
        }

        /// <summary>
        /// Gets size of the datum reader cache for testing purpose
        /// </summary>
        public int CacheSize => _datumReaderCache.Count;

        /// <summary>
        /// Deserialize the bytes to the original Avro message given the schema retrieved
        /// from the schema registry.
        /// </summary>
        /// <param name="data">data to be de-serialized</param>
        /// <param name="schema">Avro schema</param>
        /// <returns>de-serialized object</returns>
        /// <exception cref="AwsSchemaRegistryException">Exception during de-serialization.</exception>
        public object Deserialize([NotNull] byte[] data, [NotNull] GlueSchemaRegistrySchema schema)
        {
            try
            {
                var schemaDefinition = schema.SchemaDef;

                var datumReader = GetDatumReader(schemaDefinition);
                var memoryStream = new MemoryStream(data);
                var binaryDecoder = new BinaryDecoder(memoryStream);
                return datumReader.Read(reuse: null, binaryDecoder);
            }
            catch (Exception e)
            {
                const string message = "Exception occurred while de-serializing Avro message";
                throw new AwsSchemaRegistryException(message, e);
            }
        }

        private DatumReader<object> GetDatumReader(string schema)
        {
            return _datumReaderCache.GetOrCreate(
                schema,
                NewDatumReader);
        }

        private DatumReader<object> NewDatumReader(ICacheEntry entry)
        {
            entry.Size = 1;
            var schema = entry.Key as string;
            var schemaObject = Schema.Parse(schema);

            return _avroRecordType switch
            {
                AvroRecordType.GenericRecord =>
                    new GenericDatumReader<object>(schemaObject, schemaObject),
                AvroRecordType.SpecificRecord =>
                    new SpecificDatumReader<object>(schemaObject, schemaObject),
                _ => throw new AwsSchemaRegistryException($"Unsupported AvroRecordType: {_avroRecordType}"),
            };
        }
    }
}
