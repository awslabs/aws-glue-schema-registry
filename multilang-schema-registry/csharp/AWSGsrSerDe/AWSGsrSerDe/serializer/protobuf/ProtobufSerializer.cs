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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Google.Protobuf;

namespace AWSGsrSerDe.serializer.protobuf
{
    /// <summary>
    /// Avro serialization helper.
    /// </summary>
    public class ProtobufSerializer : IDataFormatSerializer
    {
        /// <inheritdoc />
        public byte[] Serialize(object data)
        {
            Validate(data);
            try
            {
                var protobufMessage = (IMessage)data;
                var stream = new MemoryStream();
                protobufMessage.WriteTo(stream);
                var serializedBytes = stream.ToArray();
                stream.Dispose();
                return serializedBytes;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException("Could not serialize from the type provided", e);
            }
        }

        /// <summary>
        /// There is no easy way to return Protobuf schema definition in C#, we will return a Base64 encoded
        /// Protobuf data that can be parsed later.
        /// </summary>
        /// <param name="data">Protobuf message object</param>
        /// <returns>Base64 Encoded ByteString of the file descriptor</returns>
        /// <exception cref="AwsSchemaRegistryException">Exception during getting Base64 encoded Protobuf Schema.</exception>
        public string GetSchemaDefinition(object data)
        {
            Validate(data);
            try
            {
                var message = (IMessage)data;
                var fileDescriptorSerializedString = message.Descriptor.File.SerializedData.ToBase64();
                return fileDescriptorSerializedString;
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException("Could not generate schema from the type provided", e);
            }
        }

        /// <inheritdoc />
        public void Validate(string schemaDefinition, byte[] data)
        {
            // TODO: Implement
            // Left blank as the schema string representation has not been solidified
        }

        /// <summary>
        /// Validate the data format object to ensure it conforms to schema if implementation supports it.
        /// </summary>
        /// <param name="data">DataFormat specific object.</param>
        /// <exception cref="ArgumentNullException">Exception if data is null.</exception>
        /// <exception cref="AwsSchemaRegistryException">Exception if data is not a Protobuf Message.</exception>
        public void Validate([NotNull] object data)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (!(data is IMessage))
            {
                throw new AwsSchemaRegistryException($"Object is not of Message type: {data.GetType()}");
            }
        }

        /// <summary>
        /// Add following additional schema information for Protobuf message:
        /// 1. Protobuf descriptor fullname of the message
        /// </summary>
        /// <param name="data">Protobuf message object</param>
        /// <param name="schema">Schema object to add addition schema info</param>
        public void SetAdditionalSchemaInfo(object data, ref GlueSchemaRegistrySchema schema)
        {
            var protobufMessage = (IMessage)data;
            schema.SetAdditionalSchemaInfo(protobufMessage.Descriptor.FullName);
        }
    }
}
