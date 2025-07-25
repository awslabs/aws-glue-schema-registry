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
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace AWSGsrSerDe.common
{
    /// <summary>
    /// Glue Schema Registry Configuration entries.
    /// </summary>
    public class GlueSchemaRegistryConfiguration
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistryConfiguration"/> class.
        /// Build Configuration object from config elements
        /// </summary>
        /// <param name="configs">non-nested Dictionary contains key-value pairs for configuration</param>
        public GlueSchemaRegistryConfiguration(Dictionary<string, dynamic> configs)
        {
            BuildConfigs(configs);
        }
        
        /// <summary>
        /// Gets the data format configured
        /// </summary>
        public GlueSchemaRegistryConstants.DataFormat DataFormat { get; private set;  }

        /// <summary>
        /// Gets the Avro Record Type configured
        /// </summary>
        public AvroRecordType AvroRecordType { get; private set; }

        /// <summary>
        /// Gets the Protobuf Message Descriptor provided
        /// </summary>
        public MessageDescriptor ProtobufMessageDescriptor { get; private set; }
        
        /// <summary>
        /// Gets the Object type that the Json message should be de-serialized to
        /// if this value is not set, Json message will be de-serialized into plain Json
        /// </summary>
        public Type JsonObjectType  { get; private set; }

        private void BuildConfigs(Dictionary<string, dynamic> configs)
        {
            ValidateAndSetAvroRecordType(configs);
            ValidateAndSetProtobufMessageDescriptor(configs);
            ValidateAndSetDataFormat(configs);
            ValidateAndSetJsonObjectType(configs);
        }

        private void ValidateAndSetDataFormat(Dictionary<string, dynamic> configs)
        {
            if (configs.ContainsKey(GlueSchemaRegistryConstants.DataFormatType))
            {
                DataFormat = configs[GlueSchemaRegistryConstants.DataFormatType];
            }
        }

        private void ValidateAndSetAvroRecordType(Dictionary<string, dynamic> configs)
        {
            if (configs.ContainsKey(GlueSchemaRegistryConstants.AvroRecordType))
            {
                AvroRecordType = configs[GlueSchemaRegistryConstants.AvroRecordType];
            }
            else
            {
                // Set a sensible default when not specified
                AvroRecordType = AvroRecordType.GenericRecord;
            }
        }

        private void ValidateAndSetProtobufMessageDescriptor(Dictionary<string, dynamic> configs)
        {
            if (configs.ContainsKey(GlueSchemaRegistryConstants.ProtobufMessageDescriptor))
            {
                ProtobufMessageDescriptor = configs[GlueSchemaRegistryConstants.ProtobufMessageDescriptor];
            }
        }

        private void ValidateAndSetJsonObjectType(Dictionary<string, dynamic> configs)
        {
            if (configs.ContainsKey(GlueSchemaRegistryConstants.JsonObjectType))
            {
                JsonObjectType = configs[GlueSchemaRegistryConstants.JsonObjectType];
            }
        }
    }
}
