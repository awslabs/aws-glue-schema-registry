package json

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

func TestNewJsonSerializer(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		config := &common.Configuration{}
		serializer := NewJsonSerializer(config)
		assert.NotNil(t, serializer)
		assert.Equal(t, config, serializer.config)
	})

	t.Run("NilConfiguration", func(t *testing.T) {
		assert.Panics(t, func() {
			NewJsonSerializer(nil)
		})
	})
}

func TestJsonSerializer_Serialize(t *testing.T) {
	config := &common.Configuration{}
	serializer := NewJsonSerializer(config)

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
		name           string
		data           interface{}
		expectError    bool
		expectedResult []byte
		errorContains  string
	}{
		{
			name: "ValidJsonDataWithSchema",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, `{"name": "John", "age": 30}`)
				return wrapper
			}(),
			expectError:    false,
			expectedResult: []byte(`{"name": "John", "age": 30}`),
		},
		{
			name: "ValidJsonDataWithSchemaEmptyPayload",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, "")
				return wrapper
			}(),
			expectError:    false,
			expectedResult: []byte{},
		},
		{
			name:          "NilData",
			data:          nil,
			expectError:   true,
			errorContains: "cannot serialize nil data",
		},
		{
			name:          "InvalidDataType",
			data:          "not a JsonDataWithSchema",
			expectError:   true,
			errorContains: "JSON serializer only accepts JsonDataWithSchema wrapper objects",
		},
		{
			name:          "StructData",
			data:          struct{ Name string }{Name: "test"},
			expectError:   true,
			errorContains: "JSON serializer only accepts JsonDataWithSchema wrapper objects",
		},
		{
			name: "InvalidPayloadJson",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, `{"name": "John", "age":}`) // Invalid JSON
				return wrapper
			}(),
			expectError:   true,
			errorContains: "payload is not valid JSON",
		},
		{
			name: "PayloadDoesNotMatchSchema",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, `{"age": 30}`) // Missing required "name"
				return wrapper
			}(),
			expectError:   true,
			errorContains: "payload validation against schema failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serializer.Serialize(tt.data)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestJsonSerializer_GetSchemaDefinition(t *testing.T) {
	config := &common.Configuration{}
	serializer := NewJsonSerializer(config)

	validSchema := `{"type": "object", "properties": {"name": {"type": "string"}}}`

	tests := []struct {
		name          string
		data          interface{}
		expectError   bool
		expectedResult string
		errorContains string
	}{
		{
			name: "ValidJsonDataWithSchema",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, `{"name": "test"}`)
				return wrapper
			}(),
			expectError:    false,
			expectedResult: validSchema,
		},
		{
			name:          "NilData",
			data:          nil,
			expectError:   true,
			errorContains: "cannot get schema from nil data",
		},
		{
			name:          "InvalidDataType",
			data:          "not a JsonDataWithSchema",
			expectError:   true,
			errorContains: "JSON serializer only accepts JsonDataWithSchema wrapper objects",
		},
		{
			name: "EmptySchema",
			data: func() *JsonDataWithSchema {
				// Create with empty schema by bypassing validation
				return &JsonDataWithSchema{Schema: "", Payload: `{"name": "test"}`}
			}(),
			expectError:   true,
			errorContains: "wrapper contains empty schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serializer.GetSchemaDefinition(tt.data)

			if tt.expectError {
				assert.Error(t, err)
				assert.Empty(t, result)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestJsonSerializer_Validate(t *testing.T) {
	config := &common.Configuration{}
	serializer := NewJsonSerializer(config)

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
		name             string
		schemaDefinition string
		data             []byte
		expectError      bool
		errorContains    string
	}{
		{
			name:             "ValidSchemaAndData",
			schemaDefinition: validSchema,
			data:             []byte(`{"name": "John", "age": 30}`),
			expectError:      false,
		},
		{
			name:             "ValidSchemaMinimalData",
			schemaDefinition: validSchema,
			data:             []byte(`{"name": "John"}`),
			expectError:      false,
		},
		{
			name:             "EmptyData",
			schemaDefinition: validSchema,
			data:             []byte{},
			expectError:      false, // Empty data is considered valid
		},
		{
			name:             "NilData",
			schemaDefinition: validSchema,
			data:             nil,
			expectError:      true,
			errorContains:    "data cannot be nil",
		},
		{
			name:             "EmptySchema",
			schemaDefinition: "",
			data:             []byte(`{"name": "John"}`),
			expectError:      true,
			errorContains:    "schema definition cannot be empty",
		},
		{
			name:             "WhitespaceOnlySchema",
			schemaDefinition: "   \t\n  ",
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
			name:             "DataDoesNotMatchSchema",
			schemaDefinition: validSchema,
			data:             []byte(`{"age": 30}`), // Missing required "name"
			expectError:      true,
			errorContains:    "validation errors",
		},
		{
			name:             "InvalidJsonData",
			schemaDefinition: validSchema,
			data:             []byte(`{"name": "John", "age":}`), // Invalid JSON
			expectError:      true,
			errorContains:    "validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.Validate(tt.schemaDefinition, tt.data)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestJsonSerializer_ValidateObject(t *testing.T) {
	config := &common.Configuration{}
	serializer := NewJsonSerializer(config)

	validSchema := `{"type": "object", "properties": {"name": {"type": "string"}}}`

	tests := []struct {
		name          string
		data          interface{}
		expectError   bool
		errorContains string
	}{
		{
			name: "ValidJsonDataWithSchema",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, `{"name": "test"}`)
				return wrapper
			}(),
			expectError: false,
		},
		{
			name: "ValidJsonDataWithSchemaEmptyPayload",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, "")
				return wrapper
			}(),
			expectError: false,
		},
		{
			name:          "NilData",
			data:          nil,
			expectError:   true,
			errorContains: "data cannot be nil",
		},
		{
			name:          "InvalidDataType",
			data:          "not a JsonDataWithSchema",
			expectError:   true,
			errorContains: "JSON serializer only accepts JsonDataWithSchema wrapper objects",
		},
		{
			name: "EmptySchema",
			data: func() *JsonDataWithSchema {
				// Create with empty schema by bypassing validation
				return &JsonDataWithSchema{Schema: "", Payload: `{"name": "test"}`}
			}(),
			expectError:   true,
			errorContains: "JsonDataWithSchema contains empty schema",
		},
		{
			name: "InvalidSchemaJson",
			data: func() *JsonDataWithSchema {
				// Create with invalid JSON schema by bypassing validation
				return &JsonDataWithSchema{Schema: `{"type": "object", "properties":}`, Payload: `{"name": "test"}`}
			}(),
			expectError:   true,
			errorContains: "JsonDataWithSchema contains invalid JSON schema",
		},
		{
			name: "InvalidPayloadJson",
			data: func() *JsonDataWithSchema {
				// Create with invalid JSON payload by bypassing validation
				return &JsonDataWithSchema{Schema: validSchema, Payload: `{"name": "test",}`}
			}(),
			expectError:   true,
			errorContains: "JsonDataWithSchema payload is not valid JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.ValidateObject(tt.data)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestJsonSerializer_SetAdditionalSchemaInfo(t *testing.T) {
	config := &common.Configuration{}
	serializer := NewJsonSerializer(config)

	validSchema := `{"type": "object", "properties": {"name": {"type": "string"}}}`

	tests := []struct {
		name          string
		data          interface{}
		schema        *gsrserde.Schema
		expectError   bool
		errorContains string
	}{
		{
			name: "ValidJsonDataWithSchema",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, `{"name": "test"}`)
				return wrapper
			}(),
			schema: &gsrserde.Schema{
				Name:           "TestSchema",
				Definition:     validSchema,
				DataFormat:     "",
				AdditionalInfo: "",
			},
			expectError: false,
		},
		{
			name:          "NilData",
			data:          nil,
			schema:        &gsrserde.Schema{},
			expectError:   true,
			errorContains: "data cannot be nil",
		},
		{
			name: "NilSchema",
			data: func() *JsonDataWithSchema {
				wrapper, _ := NewJsonDataWithSchema(validSchema, `{"name": "test"}`)
				return wrapper
			}(),
			schema:        nil,
			expectError:   true,
			errorContains: "schema cannot be nil",
		},
		{
			name:          "InvalidDataType",
			data:          "not a JsonDataWithSchema",
			schema:        &gsrserde.Schema{},
			expectError:   true,
			errorContains: "JSON serializer only accepts JsonDataWithSchema wrapper objects",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.SetAdditionalSchemaInfo(tt.data, tt.schema)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, "JsonDataWithSchema", tt.schema.AdditionalInfo)
				assert.Equal(t, "JSON", tt.schema.DataFormat)
			}
		})
	}
}

func TestJsonSerializer_ComplexScenarios(t *testing.T) {
	config := &common.Configuration{}
	serializer := NewJsonSerializer(config)

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

		wrapper, err := NewJsonDataWithSchema(schema, payload)
		require.NoError(t, err)

		result, err := serializer.Serialize(wrapper)
		assert.NoError(t, err)
		assert.JSONEq(t, payload, string(result))
	})

	t.Run("SchemaValidationFailure", func(t *testing.T) {
		schema := `{
			"type": "object",
			"properties": {
				"age": {"type": "integer", "minimum": 0}
			},
			"required": ["age"]
		}`

		invalidPayload := `{"age": -5}` // Violates minimum constraint

		wrapper, err := NewJsonDataWithSchema(schema, invalidPayload)
		require.NoError(t, err)

		_, err = serializer.Serialize(wrapper)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "payload validation against schema failed")
	})
}

func TestJsonSerializer_ErrorTypes(t *testing.T) {
	config := &common.Configuration{}
	serializer := NewJsonSerializer(config)

	t.Run("JsonSerializationError", func(t *testing.T) {
		_, err := serializer.Serialize(nil)
		assert.Error(t, err)

		var serErr *JsonSerializationError
		assert.ErrorAs(t, err, &serErr)
		assert.Contains(t, serErr.Error(), "JSON serialization error")
		assert.NotNil(t, serErr.Unwrap())
	})

	t.Run("JsonValidationError", func(t *testing.T) {
		err := serializer.Validate("", []byte(`{"test": "data"}`))
		assert.Error(t, err)

		var valErr *JsonValidationError
		assert.ErrorAs(t, err, &valErr)
		assert.Contains(t, valErr.Error(), "JSON validation error")
		assert.NotNil(t, valErr.Unwrap())
	})
}

func TestJsonSerializer_ConcurrentAccess(t *testing.T) {
	config := &common.Configuration{}
	serializer := NewJsonSerializer(config)

	schema := `{"type": "object", "properties": {"id": {"type": "integer"}}}`

	// Test concurrent access to serializer
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			payload := fmt.Sprintf(`{"id": %d}`, id)
			wrapper, err := NewJsonDataWithSchema(schema, payload)
			if err != nil {
				t.Errorf("Failed to create wrapper: %v", err)
				return
			}

			result, err := serializer.Serialize(wrapper)
			if err != nil {
				t.Errorf("Failed to serialize: %v", err)
				return
			}

			assert.JSONEq(t, payload, string(result))
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
