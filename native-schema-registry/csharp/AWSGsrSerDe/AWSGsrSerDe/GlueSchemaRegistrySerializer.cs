namespace AWSGsrSerDe
{
    /// <summary>
    /// GlueSchemaRegistrySerializer class that encodes the given byte array with GSR schema headers.
    /// Schema registration / data compression can happen based on specified configuration.
    /// </summary>>
    public class GlueSchemaRegistrySerializer
    {
        private readonly glue_schema_registry_serializer _serializer;

        public GlueSchemaRegistrySerializer()
        {
            _serializer = new glue_schema_registry_serializer();
        }

        ~GlueSchemaRegistrySerializer()
        {
            _serializer.Dispose();
        }

        /// <summary>
        /// Encodes the given bytes with GlueSchemaRegistry header information.
        /// <param name="transportName">Name of the transport. Defaults to "default-stream" if not specified.</param>
        /// <param name="schema">GlueSchemaRegistrySchema object to encode with.</param>
        /// <param name="bytes">Input bytes for a message to encode with Schema Registry header information.</param>
        /// </summary>
        public byte[] Encode(string transportName, GlueSchemaRegistrySchema schema, byte[] bytes)
        {
            var glueSchemaRegistrySchema =
                new glue_schema_registry_schema(schema.SchemaName, schema.SchemaDef, schema.DataFormat);
            var readOnlyByteArr = new read_only_byte_array(bytes, (uint)bytes.Length);

            var mutableByteArray = _serializer.encode(readOnlyByteArr, transportName, glueSchemaRegistrySchema);

            var ret = new byte[mutableByteArray.get_max_len()];
            mutableByteArray.get_data_copy(ret);

            glueSchemaRegistrySchema.Dispose();
            readOnlyByteArr.Dispose();
            mutableByteArray.Dispose();
            return ret;
        }
    }
}