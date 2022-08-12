namespace AWSGsrSerDe.deserializer
{
    /// <summary>
    /// Interface for all schemaType/protocol/dataformat specific de-serializer implementations.
    /// </summary>
    public interface IDataFormatDeserializer
    {
        /// <summary>
        /// De-serializes the given ByteBuffer to an Object.
        /// </summary>
        /// <param name="data">data to de-serialize as byte array</param>
        /// <param name="schema">schema for the data</param>
        /// <returns>de-serialized object</returns>
        object Deserialize(byte[] data, GlueSchemaRegistrySchema schema);
    }
}
