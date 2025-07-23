package json

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xeipuuv/gojsonschema"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidJsonData is returned when the data is not valid JSON
	ErrInvalidJsonData = fmt.Errorf("data must be valid JSON")

	// ErrNilData is returned when a nil data is provided
	ErrNilData = fmt.Errorf("data cannot be nil")

	// ErrDeserialization is returned when JSON deserialization fails
	ErrDeserialization = fmt.Errorf("JSON deserialization failed")

	// ErrValidation is returned when JSON validation fails
	ErrValidation = fmt.Errorf("JSON validation failed")

	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("invalid JSON schema")
)

// JsonDeserializationError represents an error that occurred during JSON deserialization
type JsonDeserializationError struct {
	Message string
	Cause   error
}

func (e *JsonDeserializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("JSON deserialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("JSON deserialization error: %s", e.Message)
}

func (e *JsonDeserializationError) Unwrap() error {
	return e.Cause
}

// JsonValidationError represents an error that occurred during JSON validation
type JsonValidationError struct {
	Message string
	Cause   error
}

func (e *JsonValidationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("JSON validation error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("JSON validation error: %s", e.Message)
}

func (e *JsonValidationError) Unwrap() error {
	return e.Cause
}

// JsonDeserializer handles deserialization of JSON data with schema validation.
// It returns JsonDataWithSchema wrapper objects containing both the schema and deserialized data.
type JsonDeserializer struct {
	// This deserializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewJsonDeserializer creates a new JSON deserializer instance.
func NewJsonDeserializer(config *common.Configuration) *JsonDeserializer {
	if config == nil {
		panic("configuration cannot be nil")
	}
	return &JsonDeserializer{
		config: config,
	}
}

// Deserialize deserializes JSON data bytes and returns the validated JSON payload as a string.
// The schema parameter contains the JSON schema definition for validation.
//
// Parameters:
//
//	data: The JSON data bytes to deserialize
//	schema: The schema object containing the JSON schema definition
//
// Returns:
//
//	interface{}: The validated JSON payload as a string
//	error: Any error that occurred during deserialization
func (j *JsonDeserializer) Deserialize(data []byte, schema *gsrserde.Schema) (interface{}, error) {
	if data == nil {
		return nil, &JsonDeserializationError{
			Message: "cannot deserialize nil data",
			Cause:   ErrNilData,
		}
	}

	if schema == nil {
		return nil, &JsonDeserializationError{
			Message: "schema cannot be nil",
			Cause:   ErrInvalidSchema,
		}
	}

	// Handle empty data case
	if len(data) == 0 {
		return "", nil
	}

	// Validate that data is valid JSON
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, &JsonDeserializationError{
			Message: "data is not valid JSON",
			Cause:   err,
		}
	}

	// Get schema definition from schema object
	schemaDefinition := schema.Definition
	if strings.TrimSpace(schemaDefinition) == "" {
		return nil, &JsonDeserializationError{
			Message: "schema definition is empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Validate data against schema
	if err := j.validateAgainstSchema(schemaDefinition, data); err != nil {
		return nil, &JsonDeserializationError{
			Message: "data validation against schema failed",
			Cause:   err,
		}
	}

	// Return the validated JSON payload as a string
	return string(data), nil
}

// validateAgainstSchema validates JSON data against a schema definition using gojsonschema.
//
// Parameters:
//
//	schemaDefinition: The JSON schema definition string
//	data: The JSON data bytes to validate
//
// Returns:
//
//	error: Any validation error, nil if valid
func (j *JsonDeserializer) validateAgainstSchema(schemaDefinition string, data []byte) error {
	if data == nil {
		return &JsonValidationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	if len(data) == 0 {
		// Empty data is valid for optional fields
		return nil
	}

	if strings.TrimSpace(schemaDefinition) == "" {
		return &JsonValidationError{
			Message: "schema definition cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Load schema
	schemaLoader := gojsonschema.NewStringLoader(schemaDefinition)

	// Load document
	documentLoader := gojsonschema.NewBytesLoader(data)

	// Validate
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return &JsonValidationError{
			Message: "validation failed",
			Cause:   err,
		}
	}

	if !result.Valid() {
		// Collect validation errors
		var errorMessages []string
		for _, desc := range result.Errors() {
			errorMessages = append(errorMessages, desc.String())
		}

		return &JsonValidationError{
			Message: fmt.Sprintf("validation errors: %s", strings.Join(errorMessages, "; ")),
			Cause:   ErrValidation,
		}
	}

	return nil
}
