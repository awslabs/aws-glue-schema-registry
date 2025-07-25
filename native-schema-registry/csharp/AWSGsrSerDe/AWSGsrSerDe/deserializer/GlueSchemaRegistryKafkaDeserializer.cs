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
using System.IO;
using AWSGsrSerDe.common;

namespace AWSGsrSerDe.deserializer
{
    /// <summary>
    /// Glue Schema Registry Kafka Generic Deserializer responsible for de-serializing
    /// </summary>
    public class GlueSchemaRegistryKafkaDeserializer
    {
        private readonly DataFormatDeserializerFactory _dataFormatDeserializerFactory =
            DataFormatDeserializerFactory.GetInstance();

        private readonly GlueSchemaRegistryDeserializer _glueSchemaRegistryDeserializer;
        
        private readonly GlueSchemaRegistryConfiguration _configuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistryKafkaDeserializer"/> class.
        /// </summary>
        /// <param name="configFilePath">Path to the configuration properties file</param>
        public GlueSchemaRegistryKafkaDeserializer(string configFilePath)
        {
            _configuration = LoadConfigurationFromFile(configFilePath);
            _glueSchemaRegistryDeserializer = new GlueSchemaRegistryDeserializer(configFilePath);
        }

        /// <summary>
        /// Loads configuration from properties file and creates GlueSchemaRegistryConfiguration
        /// </summary>
        /// <param name="configFilePath">Path to the configuration properties file</param>
        /// <returns>GlueSchemaRegistryConfiguration instance</returns>
        private GlueSchemaRegistryConfiguration LoadConfigurationFromFile(string configFilePath)
        {
            if (!File.Exists(configFilePath))
            {
                throw new FileNotFoundException($"Configuration file not found: {configFilePath}");
            }
            
            var configDictionary = new Dictionary<string, dynamic>();
            var lines = File.ReadAllLines(configFilePath);
            
            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#"))
                    continue;
                    
                var equalIndex = trimmedLine.IndexOf('=');
                if (equalIndex > 0)
                {
                    var key = trimmedLine.Substring(0, equalIndex).Trim();
                    var value = trimmedLine.Substring(equalIndex + 1).Trim();
                    configDictionary[key] = value;
                }
            }
            
            return new GlueSchemaRegistryConfiguration(configDictionary);
        }

        /// <summary>
        /// De-serialize operation for de-serializing the byte array to an Object.
        /// </summary>
        /// <param name="topic">Kafka topic name</param>
        /// <param name="data">serialized data to be de-serialized in byte array</param>
        /// <returns>de-serialized object instance</returns>
        public object Deserialize(string topic, byte[] data)
        {
            if (data == null)
            {
                return null;
            }

            if (!_glueSchemaRegistryDeserializer.CanDecode(data))
            {
                throw new AwsSchemaRegistryException("Byte data cannot be decoded");
            }

            var decodedBytes = _glueSchemaRegistryDeserializer.Decode(data);
            var schemaRegistrySchema = _glueSchemaRegistryDeserializer.DecodeSchema(data);

            var dataFormat = schemaRegistrySchema.DataFormat;
            var deserializer = _dataFormatDeserializerFactory.GetDeserializer(dataFormat, _configuration);

            var result = deserializer.Deserialize(decodedBytes, schemaRegistrySchema);

            return result;
        }
    }
}
