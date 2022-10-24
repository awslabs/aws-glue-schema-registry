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

using System.Collections.Generic;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer.avro;
using AWSGsrSerDe.deserializer.protobuf;

namespace AWSGsrSerDe.deserializer
{
    /// <summary>
    /// Factory to create a new instance of protocol specific de-serializer.
    /// </summary>
    public class DataFormatDeserializerFactory
    {
        private readonly Dictionary<string, ProtobufDeserializer> _protobufDeserializerMap =
            new Dictionary<string, ProtobufDeserializer>();

        private readonly Dictionary<AvroRecordType, AvroDeserializer> _avroDeserializerMap =
            new Dictionary<AvroRecordType, AvroDeserializer>();

        private static DataFormatDeserializerFactory _dataFormatDeserializerFactoryInstance;


        private DataFormatDeserializerFactory()
        {
        }

        /// <summary>
        /// Get the singleton instance of deserializer factory
        /// </summary>
        /// <returns>singleton instance of deserializer factory</returns>
        public static DataFormatDeserializerFactory GetInstance()
        {
            return _dataFormatDeserializerFactoryInstance ??= new DataFormatDeserializerFactory();
        }

        /// <summary>
        /// Lazy initializes and returns a specific de-serializer instance.
        /// </summary>
        /// <param name="dataFormat">dataFormat for creating appropriate instance</param>
        /// <param name="configs">configuration elements for de-serializers</param>
        /// <returns>protocol specific de-serializer instance.</returns>
        /// <exception cref="AwsSchemaRegistryException">Unsupported Data format.</exception>
        public IDataFormatDeserializer GetDeserializer(string dataFormat, GlueSchemaRegistryConfiguration configs)
        {
            return dataFormat switch
            {
                nameof(GlueSchemaRegistryConstants.DataFormat.AVRO) => GetAvroDeserializer(configs),
                nameof(GlueSchemaRegistryConstants.DataFormat.PROTOBUF) => GetProtobufDeserializer(configs),
                _ => throw new AwsSchemaRegistryException($"Unsupported data format: {dataFormat}"),
            };
        }

        private AvroDeserializer GetAvroDeserializer(GlueSchemaRegistryConfiguration configs)
        {
            var key = configs.AvroRecordType;
            var avroDeserializer = _avroDeserializerMap.GetValueOrDefault(
                key,
                new AvroDeserializer(configs));
            if (!_avroDeserializerMap.ContainsKey(key))
            {
                _avroDeserializerMap.Add(key, avroDeserializer);
            }
            
            return avroDeserializer;
        }

        private ProtobufDeserializer GetProtobufDeserializer(GlueSchemaRegistryConfiguration configs)
        {
            var key = configs.ProtobufMessageDescriptor.FullName;
            var protobufDeserializer = _protobufDeserializerMap.GetValueOrDefault(
                key,
                new ProtobufDeserializer(configs));
            if (!_protobufDeserializerMap.ContainsKey(key))
            {
                _protobufDeserializerMap.Add(key, protobufDeserializer);
            }

            return protobufDeserializer;
        }
    }
}
