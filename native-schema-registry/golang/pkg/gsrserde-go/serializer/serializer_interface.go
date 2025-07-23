package serializer

import (
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
)

// DataFormatSerializer defines the interface for format-specific serializers.
// Implementations of this interface handle serialization for specific data formats
// such as Avro, Protobuf, and JSON.
type DataFormatSerializer interface {
	// Serialize takes an object and returns the serialized byte data.
	// The input object type depends on the data format:
	// - Avro: typically accepts map[string]interface{} or specific Avro record type
	// - Protobuf: accepts a proto.Message implementation
	// - JSON: accepts map[string]interface{} or custom struct
	//
	// Parameters:
	//   data: The object to serialize (type depends on format)
	//
	// Returns:
	//   []byte: The serialized byte data
	//   error: Any error that occurred during serialization
	Serialize(data interface{}) ([]byte, error)

	// GetSchemaDefinition extracts the schema definition from the given data object.
	// This is used to generate or retrieve the schema that describes the data structure.
	//
	// Parameters:
	//   data: The object to extract schema from
	//
	// Returns:
	//   string: The schema definition as a string
	//   error: Any error that occurred during schema extraction
	GetSchemaDefinition(data interface{}) (string, error)

	// Validate checks if the given data conforms to the provided schema definition.
	// This performs validation against the schema before serialization.
	//
	// Parameters:
	//   schemaDefinition: The schema definition as a string
	//   data: The serialized data to validate
	//
	// Returns:
	//   error: Any validation error, nil if valid
	Validate(schemaDefinition string, data []byte) error

	// ValidateObject checks if the given object conforms to its expected schema.
	// This performs validation on the object before serialization.
	//
	// Parameters:
	//   data: The object to validate
	//
	// Returns:
	//   error: Any validation error, nil if valid
	ValidateObject(data interface{}) error

	// SetAdditionalSchemaInfo allows the serializer to set additional metadata
	// in the schema object that may be needed for proper serialization.
	//
	// Parameters:
	//   data: The object being serialized
	//   schema: The schema object to be updated with additional info
	//
	// Returns:
	//   error: Any error that occurred during schema update
	SetAdditionalSchemaInfo(data interface{}, schema *gsrserde.Schema) error
}
