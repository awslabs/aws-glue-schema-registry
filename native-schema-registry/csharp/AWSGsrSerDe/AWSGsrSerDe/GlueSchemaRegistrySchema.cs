using System;

namespace AWSGsrSerDe
{
    /// <summary>
    /// GlueSchemaRegistrySchema specifies the schemaName, schemaDefinition and data format
    /// associated with the schema managed by Glue Schema Registry.
    /// </summary>
    public class GlueSchemaRegistrySchema
    {
        public string SchemaName { get; }
        public string SchemaDef { get; }
        public string DataFormat { get; }

        public GlueSchemaRegistrySchema(string schemaName, string schemaDef, string dataFormat)
        {
            SchemaName = schemaName;
            SchemaDef = schemaDef;
            DataFormat = dataFormat;
        }

        private bool Equals(GlueSchemaRegistrySchema other)
        {
            return SchemaName == other.SchemaName && SchemaDef == other.SchemaDef && DataFormat == other.DataFormat;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == GetType() && Equals((GlueSchemaRegistrySchema)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(SchemaName, SchemaDef, DataFormat);
        }

        public override string ToString()
        {
            return $"{nameof(SchemaName)}: {SchemaName}, {nameof(SchemaDef)}: {SchemaDef}, {nameof(DataFormat)}: {DataFormat}";
        }
    }
}