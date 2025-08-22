package json

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

func TestNewJsonDeserializer(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		config := &common.Configuration{}
		deserializer := NewJsonDeserializer(config)
		assert.NotNil(t, deserializer)
		assert.Equal(t, config, deserializer.config)
	})

	t.Run("NilConfiguration", func(t *testing.T) {
		assert.Panics(t, func() {
			NewJsonDeserializer(nil)
		})
	})
}

func TestJsonDeserializer_Deserialize(t *testing.T) {
	config := &common.Configuration{}
	deserializer := NewJsonDeserializer(config)

	validSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "number"}
		},
		"required": ["name"]
	}`

	tests := []struct {
		name          string
		data          []byte
		schema        *gsrserde.Schema
		expectError   bool
		errorContains string
		expectedResult string
	}{
		{
			name: "ValidDataAndSchema",
			data: []byte(`{"name": "John", "age": 30}`),
			schema: &gsrserde.Schema{
				Name:           "TestSchema",
				Definition:     validSchema,
				DataFormat:     "JSON",
				AdditionalInfo: "JsonDataWithSchema",
			},
			expectError: false,
			expectedResult: `{"name": "John", "age": 30}`,
		},
		{
			name: "ValidDataMinimalPayload",
			data: []byte(`{"name": "John"}`),
			schema: &gsrserde.Schema{
				Name:           "TestSchema",
				Definition:     validSchema,
				DataFormat:     "JSON",
				AdditionalInfo: "JsonDataWithSchema",
			},
			expectError: false,
			expectedResult: `{"name": "John"}`,
		},
		{
			name: "EmptyDataValidSchema",
			data: []byte{},
			schema: &gsrserde.Schema{
				Name:           "TestSchema",
				Definition:     validSchema,
				DataFormat:     "JSON",
				AdditionalInfo: "JsonDataWithSchema",
			},
			expectError: false,
			expectedResult: "",
		},
		{
			name:          "NilData",
			data:          nil,
			schema:        &gsrserde.Schema{Definition: validSchema},
			expectError:   true,
			errorContains: "cannot deserialize nil data",
		},
		{
			name:          "NilSchema",
			data:          []byte(`{"name": "John"}`),
			schema:        nil,
			expectError:   true,
			errorContains: "schema cannot be nil",
		},
		{
			name: "InvalidJsonData",
			data: []byte(`{"name": "John", "age":}`), // Invalid JSON
			schema: &gsrserde.Schema{
				Definition: validSchema,
			},
			expectError:   true,
			errorContains: "data is not valid JSON",
		},
		{
			name: "EmptySchemaDefinition",
			data: []byte(`{"name": "John"}`),
			schema: &gsrserde.Schema{
				Definition: "",
			},
			expectError:   true,
			errorContains: "schema definition is empty",
		},
		{
			name: "WhitespaceOnlySchemaDefinition",
			data: []byte(`{"name": "John"}`),
			schema: &gsrserde.Schema{
				Definition: "   \t\n  ",
			},
			expectError:   true,
			errorContains: "schema definition is empty",
		},
		{
			name: "DataDoesNotMatchSchema",
			data: []byte(`{"age": 30}`), // Missing required "name"
			schema: &gsrserde.Schema{
				Definition: validSchema,
			},
			expectError:   true,
			errorContains: "data validation against schema failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := deserializer.Deserialize(tt.data, tt.schema)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				resultStr, ok := result.(string)
				assert.True(t, ok, "Result should be a string, got %T", result)
				assert.Equal(t, tt.expectedResult, resultStr)
			}
		})
	}
}

func TestJsonDeserializer_validateAgainstSchema(t *testing.T) {
	config := &common.Configuration{}
	deserializer := NewJsonDeserializer(config)

	validSchema := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer", "minimum": 0}
		},
		"required": ["name"]
	}`

	tests := []struct {
		name             string
		schemaDefinition string
		data             []byte
		expectError      bool
		errorContains    string
	}{
		{
			name:             "ValidSchemaAndData",
			schemaDefinition: validSchema,
			data:             []byte(`{"name": "John", "age": 25}`),
			expectError:      false,
		},
		{
			name:             "NilData",
			schemaDefinition: validSchema,
			data:             nil,
			expectError:      true,
			errorContains:    "data cannot be nil",
		},
		{
			name:             "EmptyData",
			schemaDefinition: validSchema,
			data:             []byte{},
			expectError:      false, // Empty data is valid for optional fields
		},
		{
			name:             "EmptySchema",
			schemaDefinition: "",
			data:             []byte(`{"name": "John"}`),
			expectError:      true,
			errorContains:    "schema definition cannot be empty",
		},
		{
			name:             "InvalidSchema",
			schemaDefinition: `{"type": "invalid"}`,
			data:             []byte(`{"name": "John"}`),
			expectError:      true,
			errorContains:    "validation failed",
		},
		{
			name:             "DataViolatesConstraints",
			schemaDefinition: validSchema,
			data:             []byte(`{"name": "John", "age": -5}`), // Violates minimum constraint
			expectError:      true,
			errorContains:    "validation errors",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := deserializer.validateAgainstSchema(tt.schemaDefinition, tt.data)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestJsonDeserializer_ComplexScenarios(t *testing.T) {
	config := &common.Configuration{}
	deserializer := NewJsonDeserializer(config)

	t.Run("ComplexNestedSchema", func(t *testing.T) {
		schema := `{
			"$schema": "http://json-schema.org/draft-07/schema#",
			"type": "object",
			"properties": {
				"user": {
					"type": "object",
					"properties": {
						"name": {"type": "string"},
						"age": {"type": "integer", "minimum": 0},
						"email": {"type": "string", "format": "email"}
					},
					"required": ["name", "email"]
				},
				"addresses": {
					"type": "array",
					"items": {
						"type": "object",
						"properties": {
							"street": {"type": "string"},
							"city": {"type": "string"},
							"zipCode": {"type": "string", "pattern": "^[0-9]{5}$"}
						},
						"required": ["street", "city"]
					}
				}
			},
			"required": ["user"]
		}`

		payload := `{
			"user": {
				"name": "John Doe",
				"age": 30,
				"email": "john.doe@example.com"
			},
			"addresses": [
				{
					"street": "123 Main St",
					"city": "Anytown",
					"zipCode": "12345"
				},
				{
					"street": "456 Oak Ave",
					"city": "Other City"
				}
			]
		}`

		gsrSchema := &gsrserde.Schema{
			Name:           "ComplexSchema",
			Definition:     schema,
			DataFormat:     "JSON",
			AdditionalInfo: "JsonDataWithSchema",
		}

		result, err := deserializer.Deserialize([]byte(payload), gsrSchema)
		require.NoError(t, err)

		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		assert.JSONEq(t, payload, resultStr)
	})

	t.Run("ArraySchema", func(t *testing.T) {
		schema := `{
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"id": {"type": "integer"},
					"name": {"type": "string"}
				},
				"required": ["id", "name"]
			}
		}`

		payload := `[
			{"id": 1, "name": "Item 1"},
			{"id": 2, "name": "Item 2"}
		]`

		gsrSchema := &gsrserde.Schema{
			Definition: schema,
		}

		result, err := deserializer.Deserialize([]byte(payload), gsrSchema)
		require.NoError(t, err)

		resultStr, ok := result.(string)
		require.True(t, ok, "Result should be a string")
		assert.JSONEq(t, payload, resultStr)
	})
}

func TestJsonDeserializer_ErrorTypes(t *testing.T) {
	config := &common.Configuration{}
	deserializer := NewJsonDeserializer(config)

	t.Run("JsonDeserializationError", func(t *testing.T) {
		_, err := deserializer.Deserialize(nil, &gsrserde.Schema{Definition: "{}"})
		assert.Error(t, err)

		var deserErr *JsonDeserializationError
		assert.ErrorAs(t, err, &deserErr)
		assert.Contains(t, deserErr.Error(), "JSON deserialization error")
		assert.NotNil(t, deserErr.Unwrap())
	})

	t.Run("JsonValidationError", func(t *testing.T) {
		err := deserializer.validateAgainstSchema(`{"type": "array"}`, []byte(`{"invalid": "data"}`))
		assert.Error(t, err)

		var valErr *JsonValidationError
		assert.ErrorAs(t, err, &valErr)
		assert.Contains(t, valErr.Error(), "JSON validation error")
		assert.NotNil(t, valErr.Unwrap())
	})
}

func TestJsonDeserializer_ConcurrentAccess(t *testing.T) {
	config := &common.Configuration{}
	deserializer := NewJsonDeserializer(config)

	schema := `{"type": "object", "properties": {"id": {"type": "integer"}}}`

	// Test concurrent access to deserializer
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			payload := fmt.Sprintf(`{"id": %d}`, id)
			gsrSchema := &gsrserde.Schema{
				Definition: schema,
			}

			result, err := deserializer.Deserialize([]byte(payload), gsrSchema)
			if err != nil {
				t.Errorf("Failed to deserialize: %v", err)
				return
			}

			resultStr, ok := result.(string)
			if !ok {
				t.Errorf("Expected string, got %T", result)
				return
			}

			assert.JSONEq(t, payload, resultStr)
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
