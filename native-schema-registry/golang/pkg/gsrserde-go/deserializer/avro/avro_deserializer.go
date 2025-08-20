package avro

import (
	"fmt"

	hambaavro "github.com/hamba/avro/v2"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidAvroData is returned when the data is not valid for AVRO deserialization
	ErrInvalidAvroData = fmt.Errorf("data must be valid AVRO binary")

	// ErrNilData is returned when a nil data is provided
	ErrNilData = fmt.Errorf("avro data cannot be nil")

	// ErrDeserialization is returned when AVRO deserialization fails
	ErrDeserialization = fmt.Errorf("avro deserialization failed")

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

// AvroDeserializer handles deserialization of AVRO messages using goavro.
// It deserializes AVRO binary data back to Go structs.
type AvroDeserializer struct {
	// This deserializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewAvroDeserializer creates a new AVRO deserializer instance.
func NewAvroDeserializer(config *common.Configuration) *AvroDeserializer {
	if config == nil {
		panic("configuration cannot be nil")
	}
	return &AvroDeserializer{
		config: config,
	}
}

// Deserialize deserializes AVRO binary data to Go struct using goavro.
// This method matches the DataFormatDeserializer interface.
//
// Parameters:
//
//	data: The AVRO binary data to deserialize
//	schema: The AVRO schema object
//
// Returns:
//
//	interface{}: The deserialized Go struct/data
//	error: Any error that occurred during deserialization
func (d *AvroDeserializer) Deserialize(data []byte, schema *gsrserde.Schema) (interface{}, error) {
	if len(data) == 0 {
		return nil, &AvroDeserializationError{
			Message: "cannot deserialize empty data",
			Cause:   ErrNilData,
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

	// Parse AVRO schema using hamba/avro
	avroSchema, err := hambaavro.Parse(schema.Definition)
	if err != nil {
		return nil, &AvroDeserializationError{
			Message: "failed to parse AVRO schema",
			Cause:   err,
		}
	}

	// Unmarshal the data using hamba/avro
	var result interface{}
	if err := hambaavro.Unmarshal(avroSchema, data, &result); err != nil {
		return nil, &AvroDeserializationError{
			Message: "failed to deserialize AVRO data",
			Cause:   err,
		}
	}

	return result, nil
}


// ValidateData validates AVRO binary data against a schema.
//
// Parameters:
//
//	data: The AVRO binary data to validate
//	schemaString: The AVRO schema as JSON string
//
// Returns:
//
//	error: Any validation error, nil if valid
func (d *AvroDeserializer) ValidateData(data []byte, schemaString string) error {
	if len(data) == 0 {
		return &AvroDeserializationError{
			Message: "data cannot be empty",
			Cause:   ErrNilData,
		}
	}

	if schemaString == "" {
		return &AvroDeserializationError{
			Message: "schema string cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Parse the schema
	avroSchema, err := hambaavro.Parse(schemaString)
	if err != nil {
		return &AvroDeserializationError{
			Message: "failed to parse AVRO schema",
			Cause:   err,
		}
	}

	// Try to unmarshal the data to validate it
	var result interface{}
	if err := hambaavro.Unmarshal(avroSchema, data, &result); err != nil {
		return &AvroDeserializationError{
			Message: "failed to validate AVRO data against schema",
			Cause:   err,
		}
	}

	return nil
}

// ValidateSchema validates an AVRO schema string.
//
// Parameters:
//
//	schemaString: The AVRO schema as JSON string
//
// Returns:
//
//	error: Any validation error, nil if valid
func (d *AvroDeserializer) ValidateSchema(schemaString string) error {
	if schemaString == "" {
		return &AvroDeserializationError{
			Message: "schema string cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Try to parse the schema to validate it
	_, err := hambaavro.Parse(schemaString)
	if err != nil {
		return &AvroDeserializationError{
			Message: "failed to parse AVRO schema",
			Cause:   err,
		}
	}

	return nil
}

// GetConfiguration returns the current configuration
func (d *AvroDeserializer) GetConfiguration() *common.Configuration {
	return d.config
}
