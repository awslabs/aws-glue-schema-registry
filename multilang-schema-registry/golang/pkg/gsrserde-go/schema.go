package gsrserde

import (
	"unsafe"
)

/*
#include "../../lib/include/glue_schema_registry_schema.h"
#include "../../lib/include/glue_schema_registry_error.h"
#include <stdlib.h>
*/
import "C"

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

// createGlueSchema converts a Schema to a C glue_schema_registry_schema
func createGlueSchema(schema *Schema) (*C.glue_schema_registry_schema, error) {

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	if schema == nil {
		return nil, ErrNilSchema
	}
	
	// Convert strings to C strings
	cSchemaName := C.CString(schema.Name)
	defer C.free(unsafe.Pointer(cSchemaName))
	
	cSchemaDef := C.CString(schema.Definition)
	defer C.free(unsafe.Pointer(cSchemaDef))
	
	cDataFormat := C.CString(schema.DataFormat)
	defer C.free(unsafe.Pointer(cDataFormat))
	
	cAdditionalInfo := C.CString(schema.AdditionalInfo)
	defer C.free(unsafe.Pointer(cAdditionalInfo))
	
	// Create schema using C API
	glueSchema := C.new_glue_schema_registry_schema(
		cSchemaName,
		cSchemaDef,
		cDataFormat,
		cAdditionalInfo,
		errHolder,
	)
	
	// Check if error was set
	if *errHolder != nil {
		return nil, extractError("create schema", *errHolder)
	}
	
	return glueSchema, nil
}

// extractSchemaFromGlue converts a C glue_schema_registry_schema to Schema
func extractSchemaFromGlue(glueSchema *C.glue_schema_registry_schema) *Schema {
	if glueSchema == nil {
		return nil
	}
	
	return &Schema{
		Name:           C.GoString(C.glue_schema_registry_schema_get_schema_name(glueSchema)),
		Definition:     C.GoString(C.glue_schema_registry_schema_get_schema_def(glueSchema)),
		DataFormat:     C.GoString(C.glue_schema_registry_schema_get_data_format(glueSchema)),
		AdditionalInfo: C.GoString(C.glue_schema_registry_schema_get_additional_schema_info(glueSchema)),
	}
}
