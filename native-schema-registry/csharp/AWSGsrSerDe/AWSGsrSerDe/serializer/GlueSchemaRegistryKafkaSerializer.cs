
using System;
using AWSGsrSerDe.common;

namespace AWSGsrSerDe.serializer
{
    /// <summary>
    /// Glue Schema Registry Serializer to be used with Kafka Producers.
    /// </summary>
    public class GlueSchemaRegistryKafkaSerializer
    {
        private readonly string _dataFormat;
        private readonly ISchemaNameStrategy _schemaNamingStrategy;
        private readonly GlueSchemaRegistrySerializer _glueSchemaRegistrySerializer;


        /// <summary>
        /// Constructor used by Kafka producer when passing as the property.
        /// </summary>
        public GlueSchemaRegistryKafkaSerializer()
        {
            // TODO: remove hardcode property with Config instead, we should also read Naming Strategy from configs
            _dataFormat = GlueSchemaRegistryConstants.DataFormat.AVRO.ToString();
            _schemaNamingStrategy = new DefaultSchemaNameStrategy();
            
            _glueSchemaRegistrySerializer = new GlueSchemaRegistrySerializer();
        }

        /// <summary>
        /// serializes the given Object to an byte array.
        /// </summary>
        /// <param name="data">message to serialize into byte array</param>
        /// <param name="topic">name of the Kafka topic</param>
        /// <returns>serialized byte array</returns>
        public byte[] Serialize(object data, string topic)
        {
            if (null == data)
            {
                return null;
            }
            
            var serializer = DataFormatSerializerFactory.GetInstance().GetSerializer(_dataFormat);
            
            var bytes = serializer.Serialize(data);
            var schemaDefinition = serializer.GetSchemaDefinition(data);

            var glueSchemaRegistrySchema = new GlueSchemaRegistrySchema(
                _schemaNamingStrategy.GetSchemaName(data, topic), 
                schemaDefinition, 
                _dataFormat);

            return _glueSchemaRegistrySerializer.Encode(topic, glueSchemaRegistrySchema, bytes);
        }
        
    }
}
