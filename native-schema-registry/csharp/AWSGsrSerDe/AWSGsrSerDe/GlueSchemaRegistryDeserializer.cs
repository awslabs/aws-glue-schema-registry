using System;
using System.IO;

namespace AWSGsrSerDe
{
    /// <summary>
    /// GlueSchemaRegistryDeserializer class that decodes the given byte array encoded with GSR schema headers.
    /// </summary>>
    public class GlueSchemaRegistryDeserializer : IDisposable
    {
        private readonly glue_schema_registry_deserializer _deserializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistryDeserializer"/> class with configuration file.
        /// </summary>
        /// <param name="configFilePath">Path to the configuration properties file.</param>
        public GlueSchemaRegistryDeserializer(string configFilePath)
        {
            ValidateConfigFilePath(configFilePath);
            
            try
            {
                _deserializer = new glue_schema_registry_deserializer(configFilePath, null);
            }
            catch (Exception e)
            {
                // Check for specific error conditions that should throw specific exceptions
                if (e.Message.Contains("No such file") || e.Message.Contains("does not exist"))
                {
                    throw new FileNotFoundException($"Configuration file not found: {configFilePath}", configFilePath);
                }
                if (e.Message.Contains("invalid") || e.Message.Contains("format") || e.Message.Contains("parse"))
                {
                    throw new AwsSchemaRegistryException($"Invalid configuration file format: {e.Message}");
                }
                
                throw new AwsSchemaRegistryException($"Failed to initialize deserializer: {e.Message}");
            }
        }

        /// <summary>
        /// Validates the configuration file path parameter.
        /// </summary>
        /// <param name="configFilePath">The configuration file path to validate.</param>
        private static void ValidateConfigFilePath(string configFilePath)
        {
            if (configFilePath is null)
            {
                throw new ArgumentException("Configuration file path cannot be null", nameof(configFilePath));
            }

            if (string.IsNullOrEmpty(configFilePath) || string.IsNullOrWhiteSpace(configFilePath))
            {
                throw new ArgumentException("Configuration file path cannot be empty or whitespace", nameof(configFilePath));
            }

            if (!File.Exists(configFilePath))
            {
                throw new FileNotFoundException($"Configuration file not found: {configFilePath}", configFilePath);
            }
        }

        ~GlueSchemaRegistryDeserializer()
        {
            Dispose(false);
        }

        /// <summary>
        /// Decodes the given GSR encoded byte array.
        /// </summary>
        /// <param name="encoded">Encoded byte array</param>
        /// <returns>Decoded byte array</returns>
        public byte[] Decode(byte[] encoded)
        {
            Validate(encoded);
            var readOnlyByteArr = new read_only_byte_array(encoded, (uint)encoded.Length, p_err: null);
            try
            {
                var mutableByteArray = _deserializer.decode(readOnlyByteArr, p_err: null);
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
                readOnlyByteArr.Dispose();
            }
        }

        /// <summary>
        /// Checks if the given GSR encoded byte array is valid and can be decoded.
        /// </summary>
        /// <param name="encoded">Encoded byte array</param>
        /// <returns>true / false</returns>
        public bool CanDecode(byte[] encoded)
        {
            Validate(encoded);

            var readOnlyByteArr = new read_only_byte_array(encoded, (uint)encoded.Length, p_err: null);
            try
            {
                return _deserializer.can_decode(readOnlyByteArr, p_err: null);
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
                readOnlyByteArr.Dispose();
            }
        }

        /// <summary>
        /// Decodes the byte array and returns the associated GSR Schema.
        /// </summary>
        /// <param name="encoded">Encoded byte array</param>
        /// <returns>GlueSchemaRegistrySchema object associated with the encoded byte array</returns>
        public GlueSchemaRegistrySchema DecodeSchema(byte[] encoded)
        {
            Validate(encoded);

            var readOnlyByteArr = new read_only_byte_array(encoded, (uint)encoded.Length,  p_err: null);
            try
            {
                var schema = _deserializer.decode_schema(readOnlyByteArr, p_err: null);

                var glueSchemaRegistrySchema = new GlueSchemaRegistrySchema(
                    schema.get_schema_name(),
                    schema.get_schema_def(),
                    schema.get_data_format());
                schema.Dispose();

                return glueSchemaRegistrySchema;
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
                readOnlyByteArr.Dispose();
            }
        }

        private static void Validate(byte[] encoded)
        {
            if (encoded is null || encoded.Length == 0)
            {
                throw new ArgumentException("Encoded bytes is null or Empty.", nameof(encoded));
            }
        }

        private void ReleaseUnmanagedResources()
        {
            _deserializer.Dispose();
        }

        private void Dispose(bool disposing)
        {
            ReleaseUnmanagedResources();
            if (disposing)
            {
                _deserializer?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
