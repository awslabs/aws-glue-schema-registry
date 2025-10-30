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

using System.Diagnostics.CodeAnalysis;
using AWSGsrSerDe.common;
using AWSGsrSerDe.serializer.avro;
using AWSGsrSerDe.serializer.protobuf;
using AWSGsrSerDe.serializer.json;

namespace AWSGsrSerDe.serializer
{
    /// <summary>
    /// Factory to create a new instance of protocol specific serializer.
    /// </summary>
    public class DataFormatSerializerFactory
    {
        private static DataFormatSerializerFactory _dataFormatSerializerFactoryInstance;
        
        private AvroSerializer _avroSerializer;
        private ProtobufSerializer _protobufSerializer;
        private JsonSerializer _jsonSerializer;

        private DataFormatSerializerFactory()
        {
        }

        /// <summary>
        /// Gets the singleton instance of the serializer factory
        /// </summary>
        /// <returns>the singleton instance of the serializer factory</returns>
        public static DataFormatSerializerFactory GetInstance()
        {
            return _dataFormatSerializerFactoryInstance ??= new DataFormatSerializerFactory();
        }

        /// <summary>
        /// Lazy initializes and returns a specific de-serializer instance.
        /// </summary>
        /// <param name="dataFormat">dataFormat for creating appropriate instance</param>
        /// <returns>protocol specific de-serializer instance.</returns>
        /// <exception cref="AwsSchemaRegistryException">Unsupported data format.</exception>
        public IDataFormatSerializer GetSerializer([NotNull]string dataFormat)
        {
            return dataFormat switch
            {
                nameof(GlueSchemaRegistryConstants.DataFormat.AVRO) => GetAvroSerializer(),
                nameof(GlueSchemaRegistryConstants.DataFormat.PROTOBUF) => GetProtobufSerializer(),
                nameof(GlueSchemaRegistryConstants.DataFormat.JSON) => GetJsonSerializer(),
                _ => throw new AwsSchemaRegistryException($"Unsupported data format: {dataFormat}"),
            };
        }
        
        private AvroSerializer GetAvroSerializer()
        {
            return _avroSerializer ??= new AvroSerializer();
        }

        private ProtobufSerializer GetProtobufSerializer()
        {
            return _protobufSerializer ??= new ProtobufSerializer();
        }

        private JsonSerializer GetJsonSerializer()
        {
            return _jsonSerializer ??= new JsonSerializer();
        }
    }
}
