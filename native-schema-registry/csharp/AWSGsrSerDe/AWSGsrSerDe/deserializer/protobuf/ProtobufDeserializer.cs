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
using System.Data;
using System.Diagnostics.CodeAnalysis;
using Avro;
using AWSGsrSerDe.common;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace AWSGsrSerDe.deserializer.protobuf
{
    /// <inheritdoc />
    public class ProtobufDeserializer : IDataFormatDeserializer
    {
        private readonly MessageDescriptor _descriptor;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtobufDeserializer"/> class.
        /// </summary>
        /// <param name="config">configuration element</param>
        public ProtobufDeserializer([NotNull] GlueSchemaRegistryConfiguration config)
        {
            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            _descriptor = config.ProtobufMessageDescriptor ??
                          throw new AwsSchemaRegistryException("Protobuf Message descriptor is null");
        }

        /// <inheritdoc />
        public object Deserialize([NotNull] byte[] data, [NotNull] GlueSchemaRegistrySchema schema)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (schema is null)
            {
                throw new ArgumentNullException(nameof(schema));
            }

            try
            {
                var message = _descriptor.Parser.ParseFrom(data);
                return message;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException("Exception occurred while de-serializing Protobuf message", e);
            }
        }
    }
}
