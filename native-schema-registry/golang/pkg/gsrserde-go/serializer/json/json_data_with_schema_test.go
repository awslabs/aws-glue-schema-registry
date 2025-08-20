package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJsonDataWithSchema(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		payload     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "ValidSchemaAndPayload",
			schema:      `{"type": "object", "properties": {"name": {"type": "string"}}}`,
			payload:     `{"name": "test"}`,
			expectError: false,
		},
		{
			name:        "ValidSchemaEmptyPayload",
			schema:      `{"type": "object"}`,
			payload:     "",
			expectError: false,
		},
		{
			name:        "EmptySchema",
			schema:      "",
			payload:     `{"name": "test"}`,
			expectError: true,
			errorMsg:    "schema cannot be blank/empty/null",
		},
		{
			name:        "WhitespaceOnlySchema",
			schema:      "   \t\n  ",
			payload:     `{"name": "test"}`,
			expectError: true,
			errorMsg:    "schema cannot be blank/empty/null",
		},
		{
			name:        "ValidSchemaAndNilPayload",
			schema:      `{"type": "string"}`,
			payload:     "",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewJsonDataWithSchema(tt.schema, tt.payload)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, tt.schema, result.GetSchema())
				assert.Equal(t, tt.payload, result.GetPayload())
			}
		})
	}
}

func TestJsonDataWithSchema_GetSchema(t *testing.T) {
	schema := `{"type": "object", "properties": {"name": {"type": "string"}}}`
	payload := `{"name": "test"}`

	wrapper, err := NewJsonDataWithSchema(schema, payload)
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	assert.Equal(t, schema, wrapper.GetSchema())
}

func TestJsonDataWithSchema_GetPayload(t *testing.T) {
	schema := `{"type": "object", "properties": {"name": {"type": "string"}}}`
	payload := `{"name": "test"}`

	wrapper, err := NewJsonDataWithSchema(schema, payload)
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	assert.Equal(t, payload, wrapper.GetPayload())
}

func TestJsonDataWithSchema_WithSchema(t *testing.T) {
	originalSchema := `{"type": "object", "properties": {"name": {"type": "string"}}}`
	newSchema := `{"type": "object", "properties": {"age": {"type": "number"}}}`
	payload := `{"name": "test"}`

	wrapper, err := NewJsonDataWithSchema(originalSchema, payload)
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	newWrapper := wrapper.WithSchema(newSchema)
	assert.NotNil(t, newWrapper)
	assert.Equal(t, newSchema, newWrapper.GetSchema())
	assert.Equal(t, payload, newWrapper.GetPayload())

	// Original should be unchanged
	assert.Equal(t, originalSchema, wrapper.GetSchema())
}

func TestJsonDataWithSchema_WithPayload(t *testing.T) {
	schema := `{"type": "object", "properties": {"name": {"type": "string"}}}`
	originalPayload := `{"name": "test"}`
	newPayload := `{"name": "updated"}`

	wrapper, err := NewJsonDataWithSchema(schema, originalPayload)
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	newWrapper := wrapper.WithPayload(newPayload)
	assert.NotNil(t, newWrapper)
	assert.Equal(t, schema, newWrapper.GetSchema())
	assert.Equal(t, newPayload, newWrapper.GetPayload())

	// Original should be unchanged
	assert.Equal(t, originalPayload, wrapper.GetPayload())
}

func TestJsonDataWithSchema_String(t *testing.T) {
	schema := `{"type": "object"}`
	payload := `{"name": "test"}`

	wrapper, err := NewJsonDataWithSchema(schema, payload)
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	str := wrapper.String()
	assert.Contains(t, str, "JsonDataWithSchema")
	assert.Contains(t, str, schema)
	assert.Contains(t, str, payload)
}

func TestJsonDataWithSchema_Equals(t *testing.T) {
	schema := `{"type": "object", "properties": {"name": {"type": "string"}}}`
	payload := `{"name": "test"}`

	wrapper1, err := NewJsonDataWithSchema(schema, payload)
	require.NoError(t, err)

	wrapper2, err := NewJsonDataWithSchema(schema, payload)
	require.NoError(t, err)

	wrapper3, err := NewJsonDataWithSchema(schema, `{"name": "different"}`)
	require.NoError(t, err)

	wrapper4, err := NewJsonDataWithSchema(`{"type": "string"}`, payload)
	require.NoError(t, err)

	tests := []struct {
		name     string
		wrapper1 *JsonDataWithSchema
		wrapper2 *JsonDataWithSchema
		expected bool
	}{
		{
			name:     "SameSchemaAndPayload",
			wrapper1: wrapper1,
			wrapper2: wrapper2,
			expected: true,
		},
		{
			name:     "SameSchemaAndSameWrapper",
			wrapper1: wrapper1,
			wrapper2: wrapper1,
			expected: true,
		},
		{
			name:     "SameSchemaDifferentPayload",
			wrapper1: wrapper1,
			wrapper2: wrapper3,
			expected: false,
		},
		{
			name:     "DifferentSchemaSamePayload",
			wrapper1: wrapper1,
			wrapper2: wrapper4,
			expected: false,
		},
		{
			name:     "CompareWithNil",
			wrapper1: wrapper1,
			wrapper2: nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.wrapper1.Equals(tt.wrapper2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestJsonDataWithSchema_BuilderPattern(t *testing.T) {
	schema := `{"type": "object", "properties": {"name": {"type": "string"}}}`
	payload := `{"name": "test"}`

	// Test builder pattern chaining
	// Start with a valid initial schema, then modify it
	initialSchema := `{"type": "string"}`
	wrapper, err := NewJsonDataWithSchema(initialSchema, "")
	require.NoError(t, err)

	result := wrapper.WithSchema(schema).WithPayload(payload)

	assert.Equal(t, schema, result.GetSchema())
	assert.Equal(t, payload, result.GetPayload())
}

func TestJsonDataWithSchema_EdgeCases(t *testing.T) {
	t.Run("EmptyPayloadValidSchema", func(t *testing.T) {
		schema := `{"type": "object"}`
		wrapper, err := NewJsonDataWithSchema(schema, "")
		require.NoError(t, err)
		assert.Equal(t, schema, wrapper.GetSchema())
		assert.Equal(t, "", wrapper.GetPayload())
	})

	t.Run("ComplexSchema", func(t *testing.T) {
		schema := `{
			"type": "object",
			"properties": {
				"name": {"type": "string"},
				"age": {"type": "number"},
				"addresses": {
					"type": "array",
					"items": {
						"type": "object",
						"properties": {
							"street": {"type": "string"},
							"city": {"type": "string"}
						}
					}
				}
			},
			"required": ["name"]
		}`
		payload := `{
			"name": "John Doe",
			"age": 30,
			"addresses": [
				{"street": "123 Main St", "city": "Anytown"},
				{"street": "456 Oak Ave", "city": "Other City"}
			]
		}`

		wrapper, err := NewJsonDataWithSchema(schema, payload)
		require.NoError(t, err)
		assert.Equal(t, schema, wrapper.GetSchema())
		assert.Equal(t, payload, wrapper.GetPayload())
	})
}
