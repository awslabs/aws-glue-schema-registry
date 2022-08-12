using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer.avro;

namespace AWSGsrSerDe.deserializer
{
    /// <summary>
    /// Factory to create a new instance of protocol specific de-serializer.
    /// </summary>
    public class DataFormatDeserializerFactory
    {
        private static DataFormatDeserializerFactory _dataFormatDeserializerFactoryInstance;
        private AvroDeserializer _avroDeserializer;

        /// <summary>
        /// Get the singleton instance of deserializer factory
        /// </summary>
        /// <returns>singleton instance of deserializer factory</returns>
        public static DataFormatDeserializerFactory GetInstance()
        {
            return _dataFormatDeserializerFactoryInstance ??= new DataFormatDeserializerFactory();
        }

        private DataFormatDeserializerFactory()
        {
        }

        /// <summary>
        /// Lazy initializes and returns a specific de-serializer instance.
        /// </summary>
        /// <param name="dataFormat">dataFormat for creating appropriate instance</param>
        /// <param name="configs">configuration elements for de-serializers</param>
        /// <returns>protocol specific de-serializer instance.</returns>
        /// <exception cref="AwsSchemaRegistryException">Unsupported Data format</exception>
        public IDataFormatDeserializer GetDeserializer(string dataFormat, GlueSchemaRegistryConfiguration configs)
        {
            return dataFormat switch
            {
                nameof(GlueSchemaRegistryConstants.DataFormat.AVRO)=> GetAvroDeserializer(configs),
                _ => throw new AwsSchemaRegistryException($"Unsupported data format: {dataFormat}")
            };
        }


        private AvroDeserializer GetAvroDeserializer(GlueSchemaRegistryConfiguration configs)
        {
            return _avroDeserializer ??= new AvroDeserializer(configs);
        }
    }
}
