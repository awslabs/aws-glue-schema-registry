package json

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	jsonserializer "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/json"
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
		validateResult func(t *testing.T, result interface{})
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
			validateResult: func(t *testing.T, result interface{}) {
				wrapper, ok := result.(*jsonserializer.JsonDataWithSchema)
				assert.True(t, ok, "Result should be JsonDataWithSchema")
				assert.Equal(t, validSchema, wrapper.GetSchema())
				assert.JSONEq(t, `{"name": "John", "age": 30}`, wrapper.GetPayload())
			},
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
			validateResult: func(t *testing.T, result interface{}) {
				wrapper, ok := result.(*jsonserializer.JsonDataWithSchema)
				assert.True(t, ok, "Result should be JsonDataWithSchema")
				assert.Equal(t, validSchema, wrapper.GetSchema())
				assert.JSONEq(t, `{"name": "John"}`, wrapper.GetPayload())
			},
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
			validateResult: func(t *testing.T, result interface{}) {
				wrapper, ok := result.(*jsonserializer.JsonDataWithSchema)
				assert.True(t, ok, "Result should be JsonDataWithSchema")
				assert.Equal(t, validSchema, wrapper.GetSchema())
				assert.Equal(t, "", wrapper.GetPayload())
			},
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
				if tt.validateResult != nil {
					tt.validateResult(t, result)
				}
			}
		})
	}
}

func TestJsonDeserializer_DeserializeToMap(t *testing.T) {
	config := &common.Configuration{}
	deserializer := NewJsonDeserializer(config)

	validSchema := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "number"}
		}
	}`

	tests := []struct {
		name          string
		data          []byte
		schema        *gsrserde.Schema
		expectError   bool
		errorContains string
		expectedMap   map[string]interface{}
	}{
		{
			name: "ValidDataAndSchema",
			data: []byte(`{"name": "John", "age": 30}`),
			schema: &gsrserde.Schema{
				Definition: validSchema,
			},
			expectError: false,
			expectedMap: map[string]interface{}{
				"name": "John",
				"age":  float64(30), // JSON numbers are parsed as float64
			},
		},
		{
			name: "ValidDataNoSchemaValidation",
			data: []byte(`{"name": "John", "age": 30}`),
			schema: &gsrserde.Schema{
				Definition: "", // Empty schema should skip validation
			},
			expectError: false,
			expectedMap: map[string]interface{}{
				"name": "John",
				"age":  float64(30),
			},
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
			errorContains: "data validation against schema failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := deserializer.DeserializeToMap(tt.data, tt.schema)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMap, result)
			}
		})
	}
}

func TestJsonDeserializer_DeserializeToStruct(t *testing.T) {
	config := &common.Configuration{}
	deserializer := NewJsonDeserializer(config)

	validSchema := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "number"}
		}
	}`

	type TestStruct struct {
		Name string  `json:"name"`
		Age  float64 `json:"age"`
	}

	tests := []struct {
		name          string
		data          []byte
		schema        *gsrserde.Schema
		target        interface{}
		expectError   bool
		errorContains string
		expectedStruct TestStruct
	}{
		{
			name: "ValidDataAndSchema",
			data: []byte(`{"name": "John", "age": 30}`),
			schema: &gsrserde.Schema{
				Definition: validSchema,
			},
			target:      &TestStruct{},
			expectError: false,
			expectedStruct: TestStruct{
				Name: "John",
				Age:  30,
			},
		},
		{
			name: "ValidDataNoSchemaValidation",
			data: []byte(`{"name": "John", "age": 30}`),
			schema: &gsrserde.Schema{
				Definition: "", // Empty schema should skip validation
			},
			target:      &TestStruct{},
			expectError: false,
			expectedStruct: TestStruct{
				Name: "John",
				Age:  30,
			},
		},
		{
			name:          "NilData",
			data:          nil,
			schema:        &gsrserde.Schema{Definition: validSchema},
			target:        &TestStruct{},
			expectError:   true,
			errorContains: "cannot deserialize nil data",
		},
		{
			name:          "NilSchema",
			data:          []byte(`{"name": "John"}`),
			schema:        nil,
			target:        &TestStruct{},
			expectError:   true,
			errorContains: "schema cannot be nil",
		},
		{
			name: "NilTarget",
			data: []byte(`{"name": "John"}`),
			schema: &gsrserde.Schema{
				Definition: validSchema,
			},
			target:        nil,
			expectError:   true,
			errorContains: "target cannot be nil",
		},
		{
			name: "InvalidJsonData",
			data: []byte(`{"name": "John", "age":}`), // Invalid JSON
			schema: &gsrserde.Schema{
				Definition: validSchema,
			},
			target:        &TestStruct{},
			expectError:   true,
			errorContains: "data validation against schema failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := deserializer.DeserializeToStruct(tt.data, tt.schema, tt.target)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
				if targetStruct, ok := tt.target.(*TestStruct); ok {
					assert.Equal(t, tt.expectedStruct, *targetStruct)
				}
			}
		})
	}
}

func TestJsonDeserializer_ValidateJsonData(t *testing.T) {
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
		name             string
		data             []byte
		schemaDefinition string
		expectError      bool
		errorContains    string
	}{
		{
			name:             "ValidDataAndSchema",
			data:             []byte(`{"name": "John", "age": 30}`),
			schemaDefinition: validSchema,
			expectError:      false,
		},
		{
			name:             "ValidDataMinimal",
			data:             []byte(`{"name": "John"}`),
			schemaDefinition: validSchema,
			expectError:      false,
		},
		{
			name:             "EmptyData",
			data:             []byte{},
			schemaDefinition: validSchema,
			expectError:      false, // Empty data is considered valid
		},
		{
			name:             "DataDoesNotMatchSchema",
			data:             []byte(`{"age": 30}`), // Missing required "name"
			schemaDefinition: validSchema,
			expectError:      true,
			errorContains:    "validation errors",
		},
		{
			name:             "InvalidJsonData",
			data:             []byte(`{"name": "John", "age":}`), // Invalid JSON
			schemaDefinition: validSchema,
			expectError:      true,
			errorContains:    "validation failed",
		},
		{
			name:             "EmptySchema",
			data:             []byte(`{"name": "John"}`),
			schemaDefinition: "",
			expectError:      true,
			errorContains:    "schema definition cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := deserializer.ValidateJsonData(tt.data, tt.schemaDefinition)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
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

		wrapper, ok := result.(*jsonserializer.JsonDataWithSchema)
		require.True(t, ok, "Result should be JsonDataWithSchema")
		assert.Equal(t, schema, wrapper.GetSchema())
		assert.JSONEq(t, payload, wrapper.GetPayload())
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

		wrapper, ok := result.(*jsonserializer.JsonDataWithSchema)
		require.True(t, ok, "Result should be JsonDataWithSchema")
		assert.JSONEq(t, payload, wrapper.GetPayload())
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
		err := deserializer.ValidateJsonData([]byte(`{"invalid": "data"}`), `{"type": "array"}`)
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

			wrapper, ok := result.(*jsonserializer.JsonDataWithSchema)
			if !ok {
				t.Errorf("Expected JsonDataWithSchema, got %T", result)
				return
			}

			assert.JSONEq(t, payload, wrapper.GetPayload())
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

func TestJsonDeserializer_RoundTrip(t *testing.T) {
	// Test serialization followed by deserialization
	config := &common.Configuration{}
	serializer := jsonserializer.NewJsonSerializer(config)
	deserializer := NewJsonDeserializer(config)

	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "number"},
			"active": {"type": "boolean"}
		},
		"required": ["name"]
	}`

	originalPayload := `{"name": "Alice", "age": 25, "active": true}`

	// Create wrapper and serialize
	wrapper, err := jsonserializer.NewJsonDataWithSchema(schema, originalPayload)
	require.NoError(t, err)

	serializedData, err := serializer.Serialize(wrapper)
	require.NoError(t, err)

	// Deserialize back
	gsrSchema := &gsrserde.Schema{
		Definition: schema,
	}

	result, err := deserializer.Deserialize(serializedData, gsrSchema)
	require.NoError(t, err)

	// Verify round-trip
	resultWrapper, ok := result.(*jsonserializer.JsonDataWithSchema)
	require.True(t, ok, "Result should be JsonDataWithSchema")
	assert.Equal(t, schema, resultWrapper.GetSchema())
	assert.JSONEq(t, originalPayload, resultWrapper.GetPayload())
}
