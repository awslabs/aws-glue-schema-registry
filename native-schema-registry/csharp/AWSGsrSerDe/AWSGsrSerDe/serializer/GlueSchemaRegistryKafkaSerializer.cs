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
using System.Collections.Generic;
using AWSGsrSerDe.common;

namespace AWSGsrSerDe.serializer
{
    /// <summary>
    /// Glue Schema Registry Serializer to be used with Kafka Producers.
    /// </summary>
    public class GlueSchemaRegistryKafkaSerializer
    {
        private readonly GlueSchemaRegistrySerializer _glueSchemaRegistrySerializer;
        
        private GlueSchemaRegistryConfiguration _configuration;
        private string _dataFormat;
        private ISchemaNameStrategy _schemaNamingStrategy;

        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistryKafkaSerializer"/> class.
        /// </summary>
        /// <param name="configs">configuration elements for serializer</param>
        public GlueSchemaRegistryKafkaSerializer(Dictionary<string, dynamic> configs)
        {
            Configure(configs);
            
            _glueSchemaRegistrySerializer = new GlueSchemaRegistrySerializer();
        }
        
        /// <summary>
        /// Configures the <see cref="GlueSchemaRegistryKafkaSerializer"/> instance
        /// </summary>
        /// <param name="configs">configuration elements for serializer</param>
        public void Configure(Dictionary<string, dynamic> configs)
        {
            _configuration = new GlueSchemaRegistryConfiguration(configs);
            _dataFormat = _configuration.DataFormat.ToString();
            _schemaNamingStrategy = new DefaultSchemaNameStrategy();
        }

        /// <summary>
        /// serializes the given Object to an byte array.
        /// </summary>
        /// <param name="data">message to serialize into byte array</param>
        /// <param name="topic">name of the Kafka topic</param>
        /// <returns>serialized byte array</returns>
        public byte[] Serialize(object data, string topic)
        {
            if (data == null)
            {
                return null;
            }

            var serializer = DataFormatSerializerFactory.GetInstance().GetSerializer(_dataFormat);

            var bytes = serializer.Serialize(data);
            var schemaDefinition = serializer.GetSchemaDefinition(data);

            var glueSchemaRegistrySchema = new GlueSchemaRegistrySchema(
                _schemaNamingStrategy.GetSchemaName(data, topic),
                schemaDefinition,
                _dataFormat);
            
            serializer.SetAdditionalSchemaInfo(data, ref glueSchemaRegistrySchema);

            return _glueSchemaRegistrySerializer.Encode(topic, glueSchemaRegistrySchema, bytes);
        }
    }
}
