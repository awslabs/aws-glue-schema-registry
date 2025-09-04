using System;
using System.Collections.Generic;
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
        /// Size of datum writer cache for testing purpose
        /// </summary>
        public int CacheSize => _datumWriterCache.Count;

        private AvroSerializer()
        {
            // TODO: make cache size limit configurable
            _datumWriterCache = new MemoryCache(new MemoryCacheOptions{SizeLimit = 1000});
        }

        /// <summary>
        ///  Constructor for Avro serializer that accepts configuration elements.
        /// </summary>
        /// <param name="configuration">configuration elements</param>
        public AvroSerializer(GlueSchemaRegistryConfiguration configuration = null)
        :this()
        {
        }

        /// <summary>
        /// Serialize the Avro message to bytes
        /// </summary>
        /// <param name="data">the Avro message for serialization</param>
        /// <returns>the serialized byte array</returns>
        public byte[] Serialize(object data)
        {
            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (data == null)
                return null;
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="schemaDefinition"></param>
        /// <param name="data"></param>
        public void Validate(string schemaDefinition, byte[] data)
        {
            //No-op
            //We cannot determine accurately if the data bytes match the schema as Avro bytes don't contain the field names.
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public void Validate(object data)
        {
            //No-op
            //Avro format assumes that the passed object contains schema and data that are mutually conformant.
            //We cannot validate the data against the schema.
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
            return memoryStream.ToArray();
        }

        private static Schema GetSchema(object data)
        {
            return data switch
            {
                GenericRecord record => record.Schema,
                GenericEnum record => record.Schema,
                ISpecificRecord record => record.Schema,
                GenericFixed record => record.Schema,
                _ => throw new AwsSchemaRegistryException("Unsupported Avro Data formats")
            };
        }

        private static DatumWriter<object> NewDatumWriter(object data, Schema schema)
        {
            return data switch
            {
                ISpecificRecord _ => new SpecificDatumWriter<object>(schema),
                GenericRecord _ => new GenericWriter<object>(schema),
                GenericEnum _ => new GenericWriter<object>(schema),
                GenericFixed _=> new GenericWriter<object>(schema),
                _ => throw new AwsSchemaRegistryException($"Unsupported type passed for serialization: {data}")
            };
        }
    }
}
