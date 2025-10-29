package avro

import (
	"fmt"
	"reflect"

	hambaavro "github.com/hamba/avro/v2"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidAvroData is returned when the data is not valid for AVRO serialization
	ErrInvalidAvroData = fmt.Errorf("data must be compatible with AVRO format")

	// ErrNilData is returned when a nil data is provided
	ErrNilData = fmt.Errorf("avro data cannot be nil")

	// ErrSchemaGeneration is returned when schema generation fails
	ErrSchemaGeneration = fmt.Errorf("failed to generate schema from avro data")

	// ErrSerialization is returned when AVRO serialization fails
	ErrSerialization = fmt.Errorf("avro serialization failed")

	// ErrValidation is returned when AVRO validation fails
	ErrValidation = fmt.Errorf("avro validation failed")

	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("invalid avro schema")
)

// AvroSerializationError represents an error that occurred during AVRO serialization
type AvroSerializationError struct {
	Message string
	Cause   error
}

func (e *AvroSerializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("avro serialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("avro serialization error: %s", e.Message)
}

func (e *AvroSerializationError) Unwrap() error {
	return e.Cause
}

// AvroValidationError represents an error that occurred during AVRO validation
type AvroValidationError struct {
	Message string
	Cause   error
}

func (e *AvroValidationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("avro validation error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("avro validation error: %s", e.Message)
}

func (e *AvroValidationError) Unwrap() error {
	return e.Cause
}

// AvroSerializer handles serialization of AVRO messages using goavro.
// It expects *avro.AvroRecord with embedded schema string and Go struct data.
type AvroSerializer struct {
	// This serializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewAvroSerializer creates a new AVRO serializer instance.
func NewAvroSerializer(config *common.Configuration) *AvroSerializer {
	if config == nil {
		panic("configuration cannot be nil")
	}
	return &AvroSerializer{
		config: config,
	}
}

// Serialize serializes AVRO data to bytes using goavro.
// The input data must be *avro.AvroRecord with schema string and Go struct data.
//
// Parameters:
//
//	data: Must be *avro.AvroRecord with embedded schema and data
//
// Returns:
//
//	[]byte: The serialized AVRO data
//	error: Any error that occurred during serialization
func (a *AvroSerializer) Serialize(data interface{}) ([]byte, error) {
	if data == nil {
		return nil, &AvroSerializationError{
			Message: "cannot serialize nil data",
			Cause:   ErrNilData,
		}
	}

	// Extract AvroRecord
	record, ok := data.(*avro.AvroRecord)
	if !ok {
		return nil, &AvroSerializationError{
			Message: fmt.Sprintf("expected *avro.AvroRecord, got %T", data),
			Cause:   ErrInvalidAvroData,
		}
	}

	// Parse AVRO schema using hamba/avro
	schema, err := hambaavro.Parse(record.Schema)
	if err != nil {
		return nil, &AvroSerializationError{
			Message: "failed to parse AVRO schema",
			Cause:   err,
		}
	}

	// Marshal the data using hamba/avro
	serializedData, err := hambaavro.Marshal(schema, record.Data)
	if err != nil {
		return nil, &AvroSerializationError{
			Message: "failed to serialize AVRO data",
			Cause:   err,
		}
	}

	return serializedData, nil
}

// GetSchemaDefinition extracts the schema definition from AVRO data.
// It returns the AVRO schema as a JSON string.
//
// Parameters:
//
//	data: Must be *avro.AvroRecord
//
// Returns:
//
//	string: The schema definition as JSON
//	error: Any error that occurred during schema extraction
func (a *AvroSerializer) GetSchemaDefinition(data interface{}) (string, error) {
	if data == nil {
		return "", &AvroSerializationError{
			Message: "cannot get schema from nil data",
			Cause:   ErrNilData,
		}
	}

	// Extract AvroRecord
	record, ok := data.(*avro.AvroRecord)
	if !ok {
		return "", &AvroSerializationError{
			Message: fmt.Sprintf("expected *avro.AvroRecord, got %T", data),
			Cause:   ErrInvalidAvroData,
		}
	}

	return record.Schema, nil
}

// Validate validates serialized AVRO data against a schema definition.
// This validates that the data can be deserialized using the provided schema.
//
// Parameters:
//
//	schemaDefinition: The AVRO schema definition as JSON string
//	data: The serialized AVRO data to validate
//
// Returns:
//
//	error: Any validation error, nil if valid
func (a *AvroSerializer) Validate(schemaDefinition string, data []byte) error {
	if schemaDefinition == "" {
		return &AvroValidationError{
			Message: "schema definition cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	if len(data) == 0 {
		return &AvroValidationError{
			Message: "data cannot be empty",
			Cause:   ErrInvalidAvroData,
		}
	}

	// Parse the schema
	schema, err := hambaavro.Parse(schemaDefinition)
	if err != nil {
		return &AvroValidationError{
			Message: "failed to parse schema definition",
			Cause:   err,
		}
	}

	// Try to unmarshal the data to validate it
	var result interface{}
	if err := hambaavro.Unmarshal(schema, data, &result); err != nil {
		return &AvroValidationError{
			Message: "failed to validate serialized data against schema",
			Cause:   err,
		}
	}

	return nil
}

// ValidateObject validates an AVRO data object.
// It checks if the object is compatible with AVRO format.
//
// Parameters:
//
//	data: The AVRO data object to validate
//
// Returns:
//
//	error: Any validation error, nil if valid
func (a *AvroSerializer) ValidateObject(data interface{}) error {
	if data == nil {
		return &AvroValidationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	// Extract AvroRecord
	record, ok := data.(*avro.AvroRecord)
	if !ok {
		return &AvroValidationError{
			Message: fmt.Sprintf("expected *avro.AvroRecord, got %T", data),
			Cause:   ErrInvalidAvroData,
		}
	}

	// Validate schema can be parsed
	if record.Schema == "" {
		return &AvroValidationError{
			Message: "schema cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Try to parse the schema
	_, err := hambaavro.Parse(record.Schema)
	if err != nil {
		return &AvroValidationError{
			Message: "failed to parse AVRO schema",
			Cause:   err,
		}
	}

	// Data can be nil for some AVRO types, so this is a basic validation
	return nil
}

// SetAdditionalSchemaInfo sets additional schema information in the schema object.
// For AVRO, this includes the schema type information.
//
// Parameters:
//
//	data: The AVRO data object
//	schema: The schema object to update
//
// Returns:
//
//	error: Any error that occurred during schema update
func (a *AvroSerializer) SetAdditionalSchemaInfo(data interface{}, schema *gsrserde.Schema) error {
	if data == nil {
		return &AvroSerializationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	if schema == nil {
		return &AvroSerializationError{
			Message: "schema cannot be nil",
			Cause:   fmt.Errorf("schema is nil"),
		}
	}

	// Extract AvroRecord
	record, ok := data.(*avro.AvroRecord)
	if !ok {
		return &AvroSerializationError{
			Message: fmt.Sprintf("expected *avro.AvroRecord, got %T", data),
			Cause:   ErrInvalidAvroData,
		}
	}

	// Set the schema definition from the AvroRecord
	schema.Definition = record.Schema

	// Set the data type as additional info
	schema.AdditionalInfo = reflect.TypeOf(record.Data).String()

	// Ensure DataFormat is set correctly
	if schema.DataFormat == "" {
		schema.DataFormat = "AVRO"
	}

	return nil
}
