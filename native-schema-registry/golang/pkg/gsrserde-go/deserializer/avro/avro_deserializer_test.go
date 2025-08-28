package avro

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	hambaavro "github.com/hamba/avro/v2"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// createAvroConfig creates a Configuration object for AVRO tests
func createAvroConfig() *common.Configuration {
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatAvro
	return common.NewConfiguration(configMap)
}

// createAvroData creates AVRO binary data for testing
func createAvroData(schema, data interface{}) ([]byte, error) {
	avroSchema, err := hambaavro.Parse(schema.(string))
	if err != nil {
		return nil, err
	}
	return hambaavro.Marshal(avroSchema, data)
}

func TestNewAvroDeserializer(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		config := createAvroConfig()
		deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")
		assert.NotNil(t, deserializer, "NewAvroDeserializer should return a non-nil deserializer")
		assert.Equal(t, config, deserializer.config)
	})

	t.Run("NilConfiguration", func(t *testing.T) {
		deserializer, err := NewAvroDeserializer(nil)
		assert.Nil(t, deserializer, "deserializer should be nil")
		assert.EqualError(t,err,common.ErrNilConfig.Error())
	})
}

func TestAvroDeserializer_Deserialize(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	// Test schemas
	stringSchema := `"string"`
	intSchema := `"int"`
	recordSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	// Create test data
	stringData, err := createAvroData(stringSchema, "test string")
	require.NoError(t, err)

	intData, err := createAvroData(intSchema, 42)
	require.NoError(t, err)

	recordData, err := createAvroData(recordSchema, map[string]interface{}{
		"name": "John Doe",
		"age":  30,
	})
	require.NoError(t, err)

	tests := []struct {
		name          string
		data          []byte
		schema        *gsrserde.Schema
		expectError   bool
		errorContains string
		validateResult func(t *testing.T, result interface{})
	}{
		{
			name: "ValidStringData",
			data: stringData,
			schema: &gsrserde.Schema{
				Name:       "StringSchema",
				Definition: stringSchema,
				DataFormat: "AVRO",
			},
			expectError: false,
			validateResult: func(t *testing.T, result interface{}) {
				assert.Equal(t, "test string", result)
			},
		},
		{
			name: "ValidIntData",
			data: intData,
			schema: &gsrserde.Schema{
				Name:       "IntSchema",
				Definition: intSchema,
				DataFormat: "AVRO",
			},
			expectError: false,
			validateResult: func(t *testing.T, result interface{}) {
				assert.Equal(t, int(42), result)
			},
		},
		{
			name: "ValidRecordData",
			data: recordData,
			schema: &gsrserde.Schema{
				Name:       "UserSchema",
				Definition: recordSchema,
				DataFormat: "AVRO",
			},
			expectError: false,
			validateResult: func(t *testing.T, result interface{}) {
				resultMap, ok := result.(map[string]interface{})
				assert.True(t, ok, "Result should be a map")
				assert.Equal(t, "John Doe", resultMap["name"])
				assert.Equal(t, int(30), resultMap["age"])
			},
		},
		{
			name:          "EmptyData",
			data:          []byte{},
			schema:        &gsrserde.Schema{Definition: stringSchema},
			expectError:   true,
			errorContains: "cannot deserialize empty data",
		},
		{
			name:          "NilData",
			data:          nil,
			schema:        &gsrserde.Schema{Definition: stringSchema},
			expectError:   true,
			errorContains: "cannot deserialize empty data",
		},
		{
			name:          "NilSchema",
			data:          stringData,
			schema:        nil,
			expectError:   true,
			errorContains: "schema cannot be nil",
		},
		{
			name: "EmptySchemaDefinition",
			data: stringData,
			schema: &gsrserde.Schema{
				Definition: "",
			},
			expectError:   true,
			errorContains: "schema definition cannot be empty",
		},
		{
			name: "InvalidSchemaDefinition",
			data: stringData,
			schema: &gsrserde.Schema{
				Definition: `{"type": "invalid"}`,
			},
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
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

func TestAvroDeserializer_ValidateData(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	stringSchema := `"string"`
	recordSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	// Create test data
	validStringData, err := createAvroData(stringSchema, "test")
	require.NoError(t, err)

	validRecordData, err := createAvroData(recordSchema, map[string]interface{}{
		"name": "John",
		"age":  25,
	})
	require.NoError(t, err)

	tests := []struct {
		name          string
		data          []byte
		schemaString  string
		expectError   bool
		errorContains string
	}{
		{
			name:         "ValidStringData",
			data:         validStringData,
			schemaString: stringSchema,
			expectError:  false,
		},
		{
			name:         "ValidRecordData",
			data:         validRecordData,
			schemaString: recordSchema,
			expectError:  false,
		},
		{
			name:          "EmptyData",
			data:          []byte{},
			schemaString:  stringSchema,
			expectError:   true,
			errorContains: "data cannot be empty",
		},
		{
			name:          "NilData",
			data:          nil,
			schemaString:  stringSchema,
			expectError:   true,
			errorContains: "data cannot be empty",
		},
		{
			name:          "EmptySchema",
			data:          validStringData,
			schemaString:  "",
			expectError:   true,
			errorContains: "schema string cannot be empty",
		},
		{
			name:          "InvalidSchema",
			data:          validStringData,
			schemaString:  `{"type": "invalid"}`,
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := deserializer.ValidateData(tt.data, tt.schemaString)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAvroDeserializer_ValidateSchema(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	tests := []struct {
		name          string
		schemaString  string
		expectError   bool
		errorContains string
	}{
		{
			name:         "ValidStringSchema",
			schemaString: `"string"`,
			expectError:  false,
		},
		{
			name:         "ValidIntSchema",
			schemaString: `"int"`,
			expectError:  false,
		},
		{
			name:         "ValidRecordSchema",
			schemaString: `{
				"type": "record",
				"name": "User",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "age", "type": "int"}
				]
			}`,
			expectError: false,
		},
		{
			name:         "ValidArraySchema",
			schemaString: `{"type": "array", "items": "string"}`,
			expectError:  false,
		},
		{
			name:         "ValidMapSchema",
			schemaString: `{"type": "map", "values": "int"}`,
			expectError:  false,
		},
		{
			name:         "ValidUnionSchema",
			schemaString: `["null", "string"]`,
			expectError:  false,
		},
		{
			name:          "EmptySchema",
			schemaString:  "",
			expectError:   true,
			errorContains: "schema string cannot be empty",
		},
		{
			name:          "InvalidJsonSchema",
			schemaString:  `{"type": "string", "invalid":}`,
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
		{
			name:          "InvalidAvroSchema",
			schemaString:  `{"type": "invalid"}`,
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
		{
			name:          "InvalidRecordSchema",
			schemaString:  `{"type": "record", "name": "Test"}`, // Missing fields
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := deserializer.ValidateSchema(tt.schemaString)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAvroDeserializer_GetConfiguration(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	result := deserializer.GetConfiguration()
	assert.Equal(t, config, result)
}

func TestAvroDeserializer_ComplexScenarios(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	t.Run("NestedRecordSchema", func(t *testing.T) {
		schema := `{
			"type": "record",
			"name": "Order",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "customer", "type": {
					"type": "record",
					"name": "Customer",
					"fields": [
						{"name": "name", "type": "string"},
						{"name": "email", "type": "string"}
					]
				}},
				{"name": "items", "type": {
					"type": "array",
					"items": {
						"type": "record",
						"name": "Item",
						"fields": [
							{"name": "product", "type": "string"},
							{"name": "quantity", "type": "int"},
							{"name": "price", "type": "double"}
						]
					}
				}}
			]
		}`

		testData := map[string]interface{}{
			"id": "order-123",
			"customer": map[string]interface{}{
				"name":  "John Doe",
				"email": "john@example.com",
			},
			"items": []interface{}{
				map[string]interface{}{
					"product":  "Widget A",
					"quantity": int32(2),
					"price":    19.99,
				},
				map[string]interface{}{
					"product":  "Widget B",
					"quantity": int32(1),
					"price":    29.99,
				},
			},
		}

		avroData, err := createAvroData(schema, testData)
		require.NoError(t, err)

		gsrSchema := &gsrserde.Schema{
			Name:       "OrderSchema",
			Definition: schema,
			DataFormat: "AVRO",
		}

		result, err := deserializer.Deserialize(avroData, gsrSchema)
		require.NoError(t, err)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok, "Result should be a map")

		assert.Equal(t, "order-123", resultMap["id"])

		customer, ok := resultMap["customer"].(map[string]interface{})
		require.True(t, ok, "Customer should be a map")
		assert.Equal(t, "John Doe", customer["name"])
		assert.Equal(t, "john@example.com", customer["email"])

		items, ok := resultMap["items"].([]interface{})
		require.True(t, ok, "Items should be an array")
		assert.Len(t, items, 2)

		item1, ok := items[0].(map[string]interface{})
		require.True(t, ok, "Item should be a map")
		assert.Equal(t, "Widget A", item1["product"])
		assert.Equal(t, int(2), item1["quantity"])
		assert.Equal(t, 19.99, item1["price"])
	})

	t.Run("UnionTypeSchema", func(t *testing.T) {
		schema := `{
			"type": "record",
			"name": "Message",
			"fields": [
				{"name": "content", "type": ["null", "string"]},
				{"name": "priority", "type": ["string", "int"]}
			]
		}`

		testData := map[string]interface{}{
			"content":  map[string]interface{}{"string": "Hello World"},
			"priority": map[string]interface{}{"int": int32(1)},
		}

		avroData, err := createAvroData(schema, testData)
		require.NoError(t, err)

		gsrSchema := &gsrserde.Schema{
			Definition: schema,
		}

		result, err := deserializer.Deserialize(avroData, gsrSchema)
		require.NoError(t, err)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok, "Result should be a map")

		assert.NotNil(t, resultMap["content"])
		assert.NotNil(t, resultMap["priority"])
	})

	t.Run("MapTypeSchema", func(t *testing.T) {
		schema := `{
			"type": "record",
			"name": "Config",
			"fields": [
				{"name": "settings", "type": {"type": "map", "values": "string"}}
			]
		}`

		testData := map[string]interface{}{
			"settings": map[string]interface{}{
				"theme":    "dark",
				"language": "en",
				"timezone": "UTC",
			},
		}

		avroData, err := createAvroData(schema, testData)
		require.NoError(t, err)

		gsrSchema := &gsrserde.Schema{
			Definition: schema,
		}

		result, err := deserializer.Deserialize(avroData, gsrSchema)
		require.NoError(t, err)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok, "Result should be a map")

		settings, ok := resultMap["settings"].(map[string]interface{})
		require.True(t, ok, "Settings should be a map")
		assert.Equal(t, "dark", settings["theme"])
		assert.Equal(t, "en", settings["language"])
		assert.Equal(t, "UTC", settings["timezone"])
	})
}

func TestAvroDeserializer_ErrorTypes(t *testing.T) {
	// Test AvroDeserializationError
	baseErr := fmt.Errorf("base error")
	deserErr := &AvroDeserializationError{
		Message: "test deserialization error",
		Cause:   baseErr,
	}

	assert.Contains(t, deserErr.Error(), "test deserialization error", "Error message should contain message")
	assert.Contains(t, deserErr.Error(), "base error", "Error message should contain cause")
	assert.Equal(t, baseErr, deserErr.Unwrap(), "Unwrap should return cause")

	// Test AvroDeserializationError without cause
	deserErrNoCause := &AvroDeserializationError{
		Message: "no cause error",
	}
	assert.Equal(t, "avro deserialization error: no cause error", deserErrNoCause.Error())
	assert.Nil(t, deserErrNoCause.Unwrap(), "Unwrap should return nil when no cause")

	// Test error constants
	assert.NotNil(t, ErrInvalidAvroData)
	assert.NotNil(t, ErrNilData)
	assert.NotNil(t, ErrDeserialization)
	assert.NotNil(t, ErrInvalidSchema)
}

func TestAvroDeserializer_ConcurrentAccess(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	schema := `{
		"type": "record",
		"name": "ConcurrentTest",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "value", "type": "string"}
		]
	}`

	// Test concurrent access to deserializer
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			testData := map[string]interface{}{
				"id":    int32(id),
				"value": fmt.Sprintf("test-%d", id),
			}

			avroData, err := createAvroData(schema, testData)
			if err != nil {
				t.Errorf("Failed to create AVRO data: %v", err)
				return
			}

			gsrSchema := &gsrserde.Schema{
				Definition: schema,
			}

			result, err := deserializer.Deserialize(avroData, gsrSchema)
			if err != nil {
				t.Errorf("Failed to deserialize: %v", err)
				return
			}

			resultMap, ok := result.(map[string]interface{})
			if !ok {
				t.Errorf("Expected map result, got %T", result)
				return
			}

			assert.Equal(t, int(id), resultMap["id"])
			assert.Equal(t, fmt.Sprintf("test-%d", id), resultMap["value"])
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

func TestAvroDeserializer_PrimitiveTypes(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	tests := []struct {
		name     string
		schema   string
		testData interface{}
		expected interface{}
	}{
		{
			name:     "Boolean",
			schema:   `"boolean"`,
			testData: true,
			expected: true,
		},
		{
			name:     "Int",
			schema:   `"int"`,
			testData: 42,
			expected: int(42),
		},
		{
			name:     "Long",
			schema:   `"long"`,
			testData: int64(1234567890),
			expected: int64(1234567890),
		},
		{
			name:     "Float",
			schema:   `"float"`,
			testData: float32(3.14),
			expected: float32(3.14),
		},
		{
			name:     "Double",
			schema:   `"double"`,
			testData: 3.14159,
			expected: 3.14159,
		},
		{
			name:     "String",
			schema:   `"string"`,
			testData: "hello world",
			expected: "hello world",
		},
		{
			name:     "Bytes",
			schema:   `"bytes"`,
			testData: []byte("binary data"),
			expected: []byte("binary data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			avroData, err := createAvroData(tt.schema, tt.testData)
			require.NoError(t, err)

			gsrSchema := &gsrserde.Schema{
				Definition: tt.schema,
			}

			result, err := deserializer.Deserialize(avroData, gsrSchema)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// BenchmarkAvroDeserializer_Deserialize benchmarks the deserialization performance
func BenchmarkAvroDeserializer_Deserialize(b *testing.B) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(b,err, "err should be nil")

	schema := `{
		"type": "record",
		"name": "BenchmarkRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	testData := map[string]interface{}{
		"id":    int32(123),
		"name":  "John Doe",
		"email": "john.doe@example.com",
		"age":   int32(30),
	}

	avroData, err := createAvroData(schema, testData)
	if err != nil {
		b.Fatalf("Failed to create AVRO data: %v", err)
	}

	gsrSchema := &gsrserde.Schema{
		Definition: schema,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := deserializer.Deserialize(avroData, gsrSchema)
		if err != nil {
			b.Fatalf("Deserialization failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result should not be nil")
		}
	}
}

// BenchmarkAvroDeserializer_ValidateData benchmarks data validation performance
func BenchmarkAvroDeserializer_ValidateData(b *testing.B) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(b,err, "err should be nil")

	schema := `{
		"type": "record",
		"name": "ValidationRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "data", "type": "string"}
		]
	}`

	testData := map[string]interface{}{
		"id":   int32(456),
		"data": "validation test data",
	}

	avroData, err := createAvroData(schema, testData)
	if err != nil {
		b.Fatalf("Failed to create AVRO data: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := deserializer.ValidateData(avroData, schema)
		if err != nil {
			b.Fatalf("Validation failed: %v", err)
		}
	}
}

// BenchmarkAvroDeserializer_ValidateSchema benchmarks schema validation performance
func BenchmarkAvroDeserializer_ValidateSchema(b *testing.B) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(b,err, "err should be nil")

	schema := `{
		"type": "record",
		"name": "SchemaValidationRecord",
		"fields": [
			{"name": "field1", "type": "string"},
			{"name": "field2", "type": "int"},
			{"name": "field3", "type": "boolean"}
		]
	}`

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := deserializer.ValidateSchema(schema)
		if err != nil {
			b.Fatalf("Schema validation failed: %v", err)
		}
	}
}
