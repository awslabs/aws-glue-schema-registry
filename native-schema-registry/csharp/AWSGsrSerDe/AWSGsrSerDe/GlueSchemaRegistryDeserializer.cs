namespace AWSGsrSerDe
{
    /// <summary>
    /// GlueSchemaRegistryDeserializer class that decodes the given byte array encoded with GSR schema headers.
    /// </summary>>
    public class GlueSchemaRegistryDeserializer
    {
        private readonly glue_schema_registry_deserializer _deserializer;

        public GlueSchemaRegistryDeserializer()
        {
            _deserializer = new glue_schema_registry_deserializer();
        }

        ~GlueSchemaRegistryDeserializer()
        {
            _deserializer.Dispose();
        }

        /// <summary>
        /// Decodes the given GSR encoded byte array.
        /// </summary>
        /// <param name="encoded">Encoded byte array</param>
        /// <returns>Decoded byte array</returns>
        public byte[] Decode(byte[] encoded)
        {
            var readOnlyByteArr = new read_only_byte_array(encoded, (uint) encoded.Length);
            var mutableByteArray = _deserializer.decode(readOnlyByteArr);
            var ret = new byte[mutableByteArray.get_max_len()];

            mutableByteArray.get_data_copy(ret);

            readOnlyByteArr.Dispose();
            mutableByteArray.Dispose();
            return ret;
        }

        /// <summary>
        /// Checks if the given GSR encoded byte array is valid and can be decoded.
        /// </summary>
        /// <param name="encoded">Encoded byte array</param>
        /// <returns>true / false</returns>
        public bool CanDecode(byte[] encoded)
        {
            var readOnlyByteArr = new read_only_byte_array(encoded, (uint)encoded.Length);
            var ret = _deserializer.can_decode(readOnlyByteArr);

            readOnlyByteArr.Dispose();
            return ret;
        }

        /// <summary>
        /// Decodes the byte array and returns the associated GSR Schema.
        /// </summary>
        /// <param name="encoded">Encoded byte array</param>
        /// <returns>GlueSchemaRegistrySchema object associated with the encoded byte array</returns>
        public GlueSchemaRegistrySchema DecodeSchema(byte[] encoded)
        {
            var readOnlyByteArr = new read_only_byte_array(encoded, (uint)encoded.Length);
            var schema = _deserializer.decode_schema(readOnlyByteArr);

            var gsrSchema = new GlueSchemaRegistrySchema(
                schema.get_schema_name(),
                schema.get_schema_def(),
                schema.get_data_format());

            readOnlyByteArr.Dispose();
            schema.Dispose();

            return gsrSchema;
        }
    }
}