package avro

import (
	"fmt"

	"github.com/linkedin/goavro/v2"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidAvroData is returned when the data is not valid for AVRO deserialization
	ErrInvalidAvroData = fmt.Errorf("data must be valid AVRO format")

	// ErrNilData is returned when a nil data is provided
	ErrNilData = fmt.Errorf("avro data cannot be nil")

	// ErrDeserialization is returned when AVRO deserialization fails
	ErrDeserialization = fmt.Errorf("avro deserialization failed")

	// ErrValidation is returned when AVRO validation fails
	ErrValidation = fmt.Errorf("avro validation failed")

	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("invalid avro schema")
)

// AvroDeserializationError represents an error that occurred during AVRO deserialization
type AvroDeserializationError struct {
	Message string
	Cause   error
}

func (e *AvroDeserializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("avro deserialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("avro deserialization error: %s", e.Message)
}

func (e *AvroDeserializationError) Unwrap() error {
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

// AvroDeserializer handles deserialization of AVRO messages.
// It uses the linkedin/goavro library for pure Go implementation.
type AvroDeserializer struct {
	// This deserializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewAvroDeserializer creates a new AVRO deserializer instance.
// The configuration contains AVRO-specific settings like record type.
func NewAvroDeserializer(config *common.Configuration) *AvroDeserializer {
	if config == nil {
		panic("configuration cannot be nil")
	}
	return &AvroDeserializer{
		config: config,
	}
}

// Deserialize deserializes AVRO data from bytes using the provided schema.
// The schema parameter must contain a valid AVRO schema definition.
//
// Parameters:
//
//	data: The serialized AVRO data bytes
//	schema: The schema object containing AVRO schema definition
//
// Returns:
//
//	interface{}: The deserialized AVRO data (typically map[string]interface{} or []interface{})
//	error: Any error that occurred during deserialization
func (a *AvroDeserializer) Deserialize(data []byte, schema *gsrserde.Schema) (interface{}, error) {
	if data == nil {
		return nil, &AvroDeserializationError{
			Message: "cannot deserialize nil data",
			Cause:   ErrNilData,
		}
	}

	if len(data) == 0 {
		return nil, &AvroDeserializationError{
			Message: "cannot deserialize empty data",
			Cause:   ErrInvalidAvroData,
		}
	}

	if schema == nil {
		return nil, &AvroDeserializationError{
			Message: "schema cannot be nil",
			Cause:   ErrInvalidSchema,
		}
	}

	if schema.Definition == "" {
		return nil, &AvroDeserializationError{
			Message: "schema definition cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Validate the data before deserialization
	if err := a.ValidateData(data, schema.Definition); err != nil {
		return nil, &AvroDeserializationError{
			Message: "data validation failed",
			Cause:   err,
		}
	}

	// Create codec from schema
	codec, err := goavro.NewCodec(schema.Definition)
	if err != nil {
		return nil, &AvroDeserializationError{
			Message: "failed to create AVRO codec from schema",
			Cause:   err,
		}
	}

	// Deserialize using goavro codec
	native, _, err := codec.NativeFromBinary(data)
	if err != nil {
		return nil, &AvroDeserializationError{
			Message: "failed to deserialize AVRO data",
			Cause:   err,
		}
	}

	return native, nil
}

// ValidateData validates serialized AVRO data against a schema definition.
// This validates that the data can be properly deserialized using the provided schema.
//
// Parameters:
//
//	data: The serialized AVRO data to validate
//	schemaDefinition: The AVRO schema definition as JSON string
//
// Returns:
//
//	error: Any validation error, nil if valid
func (a *AvroDeserializer) ValidateData(data []byte, schemaDefinition string) error {
	if data == nil {
		return &AvroValidationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	if len(data) == 0 {
		return &AvroValidationError{
			Message: "data cannot be empty",
			Cause:   ErrValidation,
		}
	}

	if schemaDefinition == "" {
		return &AvroValidationError{
			Message: "schema definition cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Create codec from schema to validate schema format
	codec, err := goavro.NewCodec(schemaDefinition)
	if err != nil {
		return &AvroValidationError{
			Message: "invalid schema definition",
			Cause:   err,
		}
	}

	// Try to deserialize the data to validate it against the schema
	_, _, err = codec.NativeFromBinary(data)
	if err != nil {
		return &AvroValidationError{
			Message: "data does not conform to schema",
			Cause:   err,
		}
	}

	return nil
}

// ValidateSchema validates an AVRO schema definition.
// It checks if the schema is a valid AVRO schema.
//
// Parameters:
//
//	schemaDefinition: The AVRO schema definition as JSON string
//
// Returns:
//
//	error: Any validation error, nil if valid
func (a *AvroDeserializer) ValidateSchema(schemaDefinition string) error {
	if schemaDefinition == "" {
		return &AvroValidationError{
			Message: "schema definition cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Try to create codec to validate schema format
	_, err := goavro.NewCodec(schemaDefinition)
	if err != nil {
		return &AvroValidationError{
			Message: "invalid schema definition",
			Cause:   err,
		}
	}

	return nil
}

// GetSchemaFromData attempts to extract or infer schema from deserialized AVRO data.
// This is a helper method for cases where you need to work backwards from data to schema.
//
// Parameters:
//
//	data: The deserialized AVRO data (interface{})
//
// Returns:
//
//	string: The inferred schema definition as JSON
//	error: Any error that occurred during schema inference
func (a *AvroDeserializer) GetSchemaFromData(data interface{}) (string, error) {
	if data == nil {
		return "", &AvroDeserializationError{
			Message: "cannot get schema from nil data",
			Cause:   ErrNilData,
		}
	}

	// This is a simplified schema inference - in production, you would typically
	// have the schema provided or stored alongside the data
	return a.inferSchema(data)
}

// inferSchema attempts to infer AVRO schema from deserialized data
func (a *AvroDeserializer) inferSchema(data interface{}) (string, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		return a.generateRecordSchema(v)
	case []interface{}:
		if len(v) == 0 {
			return `{"type": "array", "items": "null"}`, nil
		}
		itemSchema, err := a.inferSchema(v[0])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(`{"type": "array", "items": %s}`, itemSchema), nil
	case string:
		return `"string"`, nil
	case int, int32:
		return `"int"`, nil
	case int64:
		return `"long"`, nil
	case float32:
		return `"float"`, nil
	case float64:
		return `"double"`, nil
	case bool:
		return `"boolean"`, nil
	case []byte:
		return `"bytes"`, nil
	default:
		return "", fmt.Errorf("unsupported data type for schema inference: %T", data)
	}
}

// generateRecordSchema generates an AVRO record schema from a map
func (a *AvroDeserializer) generateRecordSchema(data map[string]interface{}) (string, error) {
	fields := make([]string, 0, len(data))
	
	for key, value := range data {
		fieldSchema, err := a.inferSchema(value)
		if err != nil {
			return "", fmt.Errorf("failed to infer schema for field %s: %w", key, err)
		}
		field := fmt.Sprintf(`{"name": "%s", "type": %s}`, key, fieldSchema)
		fields = append(fields, field)
	}
	
	if len(fields) == 0 {
		return `{"type": "record", "name": "EmptyRecord", "fields": []}`, nil
	}
	
	// Join fields with commas
	fieldsStr := ""
	for i, field := range fields {
		if i > 0 {
			fieldsStr += ", "
		}
		fieldsStr += field
	}
	
	schema := fmt.Sprintf(`{
		"type": "record",
		"name": "InferredRecord",
		"fields": [%s]
	}`, fieldsStr)
	
	return schema, nil
}

// ConvertToMap converts deserialized AVRO data to a map[string]interface{}.
// This is useful for working with AVRO record data in a more convenient format.
//
// Parameters:
//
//	data: The deserialized AVRO data
//
// Returns:
//
//	map[string]interface{}: The data as a map (if it's a record)
//	error: Any error that occurred during conversion
func (a *AvroDeserializer) ConvertToMap(data interface{}) (map[string]interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		return v, nil
	case nil:
		return nil, &AvroDeserializationError{
			Message: "cannot convert nil data to map",
			Cause:   ErrNilData,
		}
	default:
		return nil, &AvroDeserializationError{
			Message: fmt.Sprintf("data type %T cannot be converted to map", data),
			Cause:   ErrInvalidAvroData,
		}
	}
}

// ConvertToSlice converts deserialized AVRO data to a []interface{}.
// This is useful for working with AVRO array data in a more convenient format.
//
// Parameters:
//
//	data: The deserialized AVRO data
//
// Returns:
//
//	[]interface{}: The data as a slice (if it's an array)
//	error: Any error that occurred during conversion
func (a *AvroDeserializer) ConvertToSlice(data interface{}) ([]interface{}, error) {
	switch v := data.(type) {
	case []interface{}:
		return v, nil
	case nil:
		return nil, &AvroDeserializationError{
			Message: "cannot convert nil data to slice",
			Cause:   ErrNilData,
		}
	default:
		return nil, &AvroDeserializationError{
			Message: fmt.Sprintf("data type %T cannot be converted to slice", data),
			Cause:   ErrInvalidAvroData,
		}
	}
}
