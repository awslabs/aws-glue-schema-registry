package json

import (
	"fmt"
	"strings"
)

// JsonDataWithSchema wraps JSON schema and payload together for serialization.
// This works similar to GenericRecord in Avro, allowing users to pass both
// schema and data to the serializer for JSON data format.
// This mirrors the Java and C# implementations of JsonDataWithSchema.
type JsonDataWithSchema struct {
	// Schema is the JSON Schema string
	Schema string

	// Payload is the JSON data/payload/document to be serialized
	Payload string
}

// NewJsonDataWithSchema creates a new JsonDataWithSchema instance.
// Returns error if schema is blank/empty/nil (following Java validation pattern).
//
// Parameters:
//   schema: JSON schema string (cannot be blank/empty/nil)
//   payload: JSON data/payload string (can be empty or nil)
//
// Returns:
//   *JsonDataWithSchema: New instance if valid
//   error: Validation error if schema is invalid
func NewJsonDataWithSchema(schema, payload string) (*JsonDataWithSchema, error) {
	if strings.TrimSpace(schema) == "" {
		return nil, fmt.Errorf("schema cannot be blank/empty/null")
	}
	
	return &JsonDataWithSchema{
		Schema:  schema,
		Payload: payload,
	}, nil
}

// GetSchema returns the JSON schema string.
// This method name matches the Java and C# implementations.
func (j *JsonDataWithSchema) GetSchema() string {
	return j.Schema
}

// GetPayload returns the JSON payload string.
// This method name matches the Java and C# implementations.
func (j *JsonDataWithSchema) GetPayload() string {
	return j.Payload
}

// WithSchema creates a new JsonDataWithSchema with the specified schema.
// This provides a fluent builder-like interface.
func (j *JsonDataWithSchema) WithSchema(schema string) *JsonDataWithSchema {
	return &JsonDataWithSchema{
		Schema:  schema,
		Payload: j.Payload,
	}
}

// WithPayload creates a new JsonDataWithSchema with the specified payload.
// This provides a fluent builder-like interface.
func (j *JsonDataWithSchema) WithPayload(payload string) *JsonDataWithSchema {
	return &JsonDataWithSchema{
		Schema:  j.Schema,
		Payload: payload,
	}
}

// String returns a string representation of the JsonDataWithSchema.
func (j *JsonDataWithSchema) String() string {
	return fmt.Sprintf("JsonDataWithSchema{Schema: %s, Payload: %s}", 
		j.Schema, j.Payload)
}

// Equals checks if two JsonDataWithSchema instances are equal.
func (j *JsonDataWithSchema) Equals(other *JsonDataWithSchema) bool {
	if other == nil {
		return false
	}
	return j.Schema == other.Schema && j.Payload == other.Payload
}
