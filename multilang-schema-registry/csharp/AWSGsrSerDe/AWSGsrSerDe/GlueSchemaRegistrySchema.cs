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

namespace AWSGsrSerDe
{
    /// <summary>
    /// GlueSchemaRegistrySchema specifies the schemaName, schemaDefinition and data format
    /// associated with the schema managed by Glue Schema Registry.
    /// </summary>
    public class GlueSchemaRegistrySchema
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistrySchema"/> class.
        /// </summary>
        /// <param name="schemaName">schema name</param>
        /// <param name="schemaDef">schema definition</param>
        /// <param name="dataFormat">data format</param>
        public GlueSchemaRegistrySchema(string schemaName, string schemaDef, string dataFormat)
        {
            SchemaName = schemaName;
            SchemaDef = schemaDef;
            DataFormat = dataFormat;
            AdditionalSchemaInfo = string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistrySchema"/> class.
        /// </summary>
        /// <param name="schemaName">schema name</param>
        /// <param name="schemaDef">schema definition</param>
        /// <param name="dataFormat">data format</param>
        /// <param name="additionalSchemaInfo">additional schema info for this schema,
        /// e.g. Protobuf Message descriptor full name</param>
        public GlueSchemaRegistrySchema(
            string schemaName, 
            string schemaDef, 
            string dataFormat,
            string additionalSchemaInfo)
            : this(schemaName, schemaDef, dataFormat)
        {
            AdditionalSchemaInfo = additionalSchemaInfo;
        }

        /// <summary>
        /// Gets schema name
        /// </summary>
        public string SchemaName { get; }

        /// <summary>
        /// Gets schema definition, for Protobuf it is the base64 encoded schemaDef
        /// </summary>
        public string SchemaDef { get; }

        /// <summary>
        /// Gets the Data format of the schema
        /// </summary>
        public string DataFormat { get; }

        /// <summary>
        /// Gets the additional information(if any) for the schema, for example,
        /// we store the Protobuf descriptor full name in this field
        /// </summary>
        public string AdditionalSchemaInfo { get; private set; }

        /// <summary>
        /// Sets the Additional schema info for this schema
        /// </summary>
        /// <param name="additionalSchemaInfo">Additional schema information</param>
        public void SetAdditionalSchemaInfo(string additionalSchemaInfo)
        {
            AdditionalSchemaInfo = additionalSchemaInfo;
        }

        private bool Equals(GlueSchemaRegistrySchema other)
        {
            return SchemaName == other.SchemaName && SchemaDef == other.SchemaDef && DataFormat == other.DataFormat;
        }

#pragma warning disable SA1202
        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj.GetType() == GetType() && Equals((GlueSchemaRegistrySchema)obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return HashCode.Combine(SchemaName, SchemaDef, DataFormat);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return
                $"{nameof(SchemaName)}: {SchemaName}, {nameof(SchemaDef)}: {SchemaDef}, {nameof(DataFormat)}: {DataFormat}";
        }
#pragma warning restore SA1202
    }
}
