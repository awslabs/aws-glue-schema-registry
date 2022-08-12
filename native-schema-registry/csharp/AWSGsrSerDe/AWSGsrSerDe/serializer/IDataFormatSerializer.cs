namespace AWSGsrSerDe.serializer
{
    /// <summary>
    /// Interface for all schemaType/protocol/dataformat specific serializer implementations.
    /// </summary>
    public interface IDataFormatSerializer
    {
        /// <summary>
        /// serializes the given Object to an byte array.
        /// </summary>
        /// <param name="data">message to serialize into byte array</param>
        /// <returns>serialized byte array</returns>
        byte[] Serialize(object data);

        /// <summary>
        /// Gets schema definition from Object
        /// </summary>
        /// <param name="data">message to serialize into byte array</param>
        /// <returns>schema definition as string</returns>
        string GetSchemaDefinition(object data);

        /// <summary>
        /// Validate the given data against the schema definition if the implementing format supports it.
        /// </summary>
        /// <param name="schemaDefinition">SchemaDefinition as String.</param>
        /// <param name="data">Data as byte array.</param>
        void Validate(string schemaDefinition, byte[] data);

        /// <summary>
        /// Validate the data format object to ensure it conforms to schema if implementation supports it.
        /// </summary>
        /// <param name="data">DataFormat specific object.</param>
        void Validate(object data);
    }
}
