using System.Collections.Generic;
using AWSGsrSerDe.common;

namespace AWSGsrSerDe.deserializer
{
    /// <summary>
    /// Glue Schema Registry Kafka Generic Deserializer responsible for de-serializing
    /// </summary>
    public class GlueSchemaRegistryKafkaDeserializer
    {
        private readonly DataFormatDeserializerFactory _dataFormatDeserializerFactory = DataFormatDeserializerFactory.GetInstance();
        private readonly GlueSchemaRegistryConfiguration _configuration;
        private readonly GlueSchemaRegistryDeserializer _glueSchemaRegistryDeserializer;

        /// <summary>
        /// Constructor used by Kafka consumer.
        /// </summary>
        /// <param name="configs">configuration elements for de-serializer</param>
        public GlueSchemaRegistryKafkaDeserializer(Dictionary<string, dynamic> configs)
        {
            _configuration = new GlueSchemaRegistryConfiguration(configs);
            _glueSchemaRegistryDeserializer = new GlueSchemaRegistryDeserializer();

        }
        
        /// <summary>
        /// De-serialize operation for de-serializing the byte array to an Object.
        /// </summary>
        /// <param name="topic">Kafka topic name</param>
        /// <param name="data">serialized data to be de-serialized in byte array</param>
        /// <returns>de-serialized object instance</returns>
        public object Deserialize(string topic, byte[] data)
        {
            if (null == data)
            {
                return null;
            }

            var decodedBytes = _glueSchemaRegistryDeserializer.Decode(data);
            var schemaRegistrySchema = _glueSchemaRegistryDeserializer.DecodeSchema(data);

            var dataFormat = schemaRegistrySchema.DataFormat;
            var deserializer = _dataFormatDeserializerFactory.GetDeserializer(dataFormat,_configuration);

            var result = deserializer.Deserialize(decodedBytes, schemaRegistrySchema);

            return result;
        }
    }
}
