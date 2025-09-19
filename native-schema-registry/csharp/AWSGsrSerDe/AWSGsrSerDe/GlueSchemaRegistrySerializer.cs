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
        /// Initializes a new instance of the <see cref="GlueSchemaRegistrySerializer"/> class with configuration file.
        /// </summary>
        /// <param name="configFilePath">Path to the configuration properties file.</param>
        public GlueSchemaRegistrySerializer(string configFilePath)
        {
            try
            {
                _serializer = new glue_schema_registry_serializer(configFilePath, GlueSchemaRegistryConstants.CSharpUserAgentString, null);
            }
            catch (Exception e)
            {
                // Check for specific error conditions that should throw specific exceptions
                if (e.Message.Contains("No such file") || e.Message.Contains("does not exist"))
                {
                    throw new FileNotFoundException($"Configuration file not found: {configFilePath}", configFilePath);
                }
                
                throw new AwsSchemaRegistryException($"Failed to initialize serializer: {e.Message}");
            }
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
