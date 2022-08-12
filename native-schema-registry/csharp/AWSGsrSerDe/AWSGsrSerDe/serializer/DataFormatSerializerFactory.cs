using System.Diagnostics.CodeAnalysis;
using AWSGsrSerDe.common;
using AWSGsrSerDe.serializer.avro;

namespace AWSGsrSerDe.serializer
{
    /// <summary>
    /// Factory to create a new instance of protocol specific serializer.
    /// </summary>
    public class DataFormatSerializerFactory
    {
        private static DataFormatSerializerFactory _dataFormatSerializerFactoryInstance;
        
        private AvroSerializer _avroSerializer;

        private DataFormatSerializerFactory()
        {
        }

        /// <summary>
        /// Gets the singleton instance of the serializer factory
        /// </summary>
        /// <returns>the singleton instance of the serializer factory</returns>
        public static DataFormatSerializerFactory GetInstance()
        {
            return _dataFormatSerializerFactoryInstance ??= new DataFormatSerializerFactory();
        }

        /// <summary>
        /// Lazy initializes and returns a specific de-serializer instance.
        /// </summary>
        /// <param name="dataFormat">dataFormat for creating appropriate instance</param>
        /// <returns>protocol specific de-serializer instance.</returns>
        /// <exception cref="AwsSchemaRegistryException">Unsupported data format</exception>
        public IDataFormatSerializer GetSerializer([NotNull]string dataFormat)
        {
            return dataFormat switch
            {
                nameof(GlueSchemaRegistryConstants.DataFormat.AVRO) => GetAvroSerializer(),
                _ => throw new AwsSchemaRegistryException($"Unsupported data format: {dataFormat}")
            };
        }
        
        private AvroSerializer GetAvroSerializer()
        {
            return _avroSerializer ??= new AvroSerializer();
        }
    }
}
