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

namespace AWSGsrSerDe.serializer
{
    /// <summary>
    /// Interface for all schemaType/protocol/data format specific serializer implementations.
    /// </summary>
    public interface IDataFormatSerializer
    {
        /// <summary>
        /// serializes the given Object to an byte array.
        /// </summary>
        /// <param name="data">message to serialize into byte array</param>
        /// <returns>serialized byte array</returns>
        byte[] Serialize(object data);

        /// <summary>
        /// Gets schema definition from Object
        /// </summary>
        /// <param name="data">message to serialize into byte array</param>
        /// <returns>schema definition as string</returns>
        string GetSchemaDefinition(object data);

        /// <summary>
        /// Validate the given data against the schema definition if the implementing format supports it.
        /// </summary>
        /// <param name="schemaDefinition">SchemaDefinition as String.</param>
        /// <param name="data">Data as byte array.</param>
        void Validate(string schemaDefinition, byte[] data);

        /// <summary>
        /// Validate the data format object to ensure it conforms to schema if implementation supports it.
        /// </summary>
        /// <param name="data">DataFormat specific object.</param>
        void Validate(object data);

        /// <summary>
        /// Supplies additional information that is needed in GlueSchemaRegistrySchema for the message serialization.
        /// </summary>
        /// <param name="data">message to serialize into byte array.</param>
        /// <param name="schema">schema object for the message that will be updated with additional info.</param>
        void SetAdditionalSchemaInfo(object data, ref GlueSchemaRegistrySchema schema);
    }
}
