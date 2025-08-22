package gsrserde

import (
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/GsrSerDe"
)

// Schema represents a schema for serialization/deserialization
type Schema struct {
	// Name of the schema
	Name string
	
	// Complete definition of the schema
	Definition string
	
	// Data format: JSON, AVRO, PROTOBUF
	DataFormat string
	
	// Additional schema information (e.g., Protobuf message descriptor full name)
	AdditionalInfo string
}

// createGlueSchema converts a Schema to a SWIG Glue_schema_registry_schema
func createGlueSchema(schema *Schema, err GsrSerDe.Glue_schema_registry_error) (GsrSerDe.Glue_schema_registry_schema, error) {
	if schema == nil {
		return nil, ErrNilSchema
	}
	
	// Create schema using SWIG-generated constructor
	glueSchema := GsrSerDe.NewGlue_schema_registry_schema(
		schema.Name,
		schema.Definition,
		schema.DataFormat,
		schema.AdditionalInfo,
		err,
	)
	
	// Check if error was set
	if err != nil && err.Swigcptr() != 0 {
		return nil, extractError("create schema", err)
	}
	
	return glueSchema, nil
}

// extractSchemaFromGlue converts a SWIG Glue_schema_registry_schema to Schema
func extractSchemaFromGlue(glueSchema GsrSerDe.Glue_schema_registry_schema) *Schema {
	if glueSchema == nil || glueSchema.Swigcptr() == 0 {
		return nil
	}
	
	return &Schema{
		Name:           glueSchema.Get_schema_name(),
		Definition:     glueSchema.Get_schema_def(),
		DataFormat:     glueSchema.Get_data_format(),
		AdditionalInfo: glueSchema.Get_additional_schema_info(),
	}
}
