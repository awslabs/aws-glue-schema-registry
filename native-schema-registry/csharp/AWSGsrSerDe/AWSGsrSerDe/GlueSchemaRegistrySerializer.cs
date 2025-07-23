// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.IO;

namespace AWSGsrSerDe
{
    /// <summary>
    /// GlueSchemaRegistrySerializer class that encodes the given byte array with GSR schema headers.
    /// Schema registration / data compression can happen based on specified configuration.
    /// </summary>>
    public class GlueSchemaRegistrySerializer : IDisposable
    {
        private readonly glue_schema_registry_serializer _serializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistrySerializer"/> class.
        /// </summary>
        public GlueSchemaRegistrySerializer()
        {
            _serializer = new glue_schema_registry_serializer(p_err: null);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistrySerializer"/> class with configuration file.
        /// </summary>
        /// <param name="configFilePath">Path to the configuration properties file.</param>
        /// <exception cref="ArgumentException">Thrown when config file path is null or empty.</exception>
        /// <exception cref="FileNotFoundException">Thrown when configuration file does not exist.</exception>
        /// <exception cref="UnauthorizedAccessException">Thrown when configuration file cannot be read.</exception>
        public GlueSchemaRegistrySerializer(string configFilePath)
        {
            if (string.IsNullOrEmpty(configFilePath))
                throw new ArgumentException("Config file path cannot be null or empty", nameof(configFilePath));
                
            if (!File.Exists(configFilePath))
                throw new FileNotFoundException($"Configuration file not found: {configFilePath}");
                
            try 
            {
                using (var fs = File.OpenRead(configFilePath)) { }
            }
            catch (UnauthorizedAccessException ex)
            {
                throw new UnauthorizedAccessException($"Cannot read configuration file: {configFilePath}", ex);
            }
            
            _serializer = new glue_schema_registry_serializer(configFilePath, null);
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="GlueSchemaRegistrySerializer"/> class.
        /// </summary>
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
        /// <returns>Encoded bytes</returns>
        public byte[] Encode(string transportName, GlueSchemaRegistrySchema schema, byte[] bytes)
        {
            Validate(schema, bytes);

            var glueSchemaRegistrySchema =
                new glue_schema_registry_schema(
                    schema.SchemaName,
                    schema.SchemaDef,
                    schema.DataFormat,
                    schema.AdditionalSchemaInfo,
                    p_err: null);
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

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
