using System;

namespace AWSGsrSerDe
{
    /// <summary>
    /// GlueSchemaRegistrySerializer class that encodes the given byte array with GSR schema headers.
    /// Schema registration / data compression can happen based on specified configuration.
    /// </summary>>
    public class GlueSchemaRegistrySerializer: IDisposable
    {
        private readonly glue_schema_registry_serializer _serializer;

        public GlueSchemaRegistrySerializer()
        {
            _serializer = new glue_schema_registry_serializer(p_err: null);
        }

        ~GlueSchemaRegistrySerializer()
        {
            Dispose(false);
        }

        /// <summary>
        /// Encodes the given bytes with GlueSchemaRegistry header information.
        /// <param name="transportName">Name of the transport. Defaults to "default-stream" if not specified.</param>
        /// <param name="schema">GlueSchemaRegistrySchema object to encode with.</param>
        /// <param name="bytes">Input bytes for a message to encode with Schema Registry header information.</param>
        /// </summary>
        public byte[] Encode(string transportName, GlueSchemaRegistrySchema schema, byte[] bytes)
        {
            Validate(schema, bytes);
           
            var glueSchemaRegistrySchema =
                new glue_schema_registry_schema(schema.SchemaName, schema.SchemaDef, schema.DataFormat, p_err: null);
            var readOnlyByteArr = new read_only_byte_array(bytes, (uint)bytes.Length, p_err: null);

            try
            {
                var mutableByteArray =
                    _serializer.encode(readOnlyByteArr, transportName, glueSchemaRegistrySchema, p_err: null);
                var ret = new byte[mutableByteArray.get_max_len()];
                mutableByteArray.get_data_copy(ret);

                mutableByteArray.Dispose();
                return ret;
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException(e.Message);
            }
            finally
            {
                glueSchemaRegistrySchema.Dispose();
                readOnlyByteArr.Dispose();
            }
        }

        private static void Validate(GlueSchemaRegistrySchema schema, byte[] bytes)
        {
            if (schema is null)
            {
                throw new ArgumentException("Schema is null", nameof(schema));
            }
            
            if (bytes is null || bytes.Length == 0)
            {
                throw new ArgumentException("bytes is null or empty", nameof(bytes));
            }
        }

        private void ReleaseUnmanagedResources()
        {
            _serializer.Dispose();
        }

        private void Dispose(bool disposing)
        {
            ReleaseUnmanagedResources();
            if (disposing)
            {
                _serializer?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}