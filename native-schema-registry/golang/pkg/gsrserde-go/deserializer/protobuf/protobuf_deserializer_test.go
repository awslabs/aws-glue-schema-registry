package protobuf

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/test_helpers"
)

func TestNewProtobufDeserializer(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	assert.NotNil(t, deserializer, "NewProtobufDeserializer should return a non-nil deserializer")
	assert.Nil(t, deserializer.messageDescriptor, "messageDescriptor should be nil initially")
}

func TestProtobufDeserializer_Deserialize_ErrorCases(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	validSchema := &gsrserde.Schema{
		Name:       "TestMessage",
		Definition: "valid-definition",
		DataFormat: "PROTOBUF",
	}

	tests := []struct {
		name        string
		data        []byte
		schema      *gsrserde.Schema
		expectedErr error
	}{
		{
			name:        "nil data",
			data:        nil,
			schema:      validSchema,
			expectedErr: ErrNilData,
		},
		{
			name:        "empty data",
			data:        []byte{},
			schema:      validSchema,
			expectedErr: ErrEmptyData,
		},
		{
			name:        "nil schema",
			data:        []byte{0x01, 0x02},
			schema:      nil,
			expectedErr: ErrNilSchema,
		},
		{
			name: "non-protobuf schema",
			data: []byte{0x01, 0x02},
			schema: &gsrserde.Schema{
				Name:       "TestMessage",
				Definition: "valid-definition",
				DataFormat: "AVRO",
			},
			expectedErr: ErrSchemaNotProtobuf,
		},
		{
			name: "empty schema definition",
			data: []byte{0x01, 0x02},
			schema: &gsrserde.Schema{
				Name:       "TestMessage",
				Definition: "",
				DataFormat: "PROTOBUF",
			},
			expectedErr: ErrInvalidSchema,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := deserializer.Deserialize(tt.data, tt.schema)
			assert.Nil(t, result, "Result should be nil on error")
			assert.ErrorIs(t, err, tt.expectedErr, "Should return expected error")
		})
	}
}

func TestProtobufDeserializer_Deserialize_InvalidSchemaDefinition(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	
	tests := []struct {
		name       string
		definition string
	}{
		{
			name:       "invalid protobuf bytes",
			definition: "invalid-protobuf-data",
		},
		{
			name:       "random bytes",
			definition: string([]byte{0xFF, 0xFE, 0xFD, 0xFC}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &gsrserde.Schema{
				Name:       "TestMessage",
				Definition: tt.definition,
				DataFormat: "PROTOBUF",
			}
			
			data := []byte{0x08, 0x96, 0x01} // Valid protobuf bytes
			result, err := deserializer.Deserialize(data, schema)
			
			assert.Nil(t, result, "Result should be nil on schema parsing error")
			assert.Error(t, err, "Should return error for invalid schema definition")
			assert.Contains(t, err.Error(), "failed to get message descriptor", "Error should mention descriptor failure")
		})
	}
}

func TestProtobufDeserializer_Deserialize_ValidMessage(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	
	// Create a simple test message descriptor
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("TestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
					{
						Name:   proto.String("name"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	// Create FileDescriptorSet
	fileDescSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}

	// Serialize the descriptor set to use as schema definition
	schemaDefBytes, err := proto.Marshal(fileDescSet)
	require.NoError(t, err, "Should marshal file descriptor set")

	schema := &gsrserde.Schema{
		Name:           "TestMessage",
		Definition:     string(schemaDefBytes),
		DataFormat:     "PROTOBUF",
		AdditionalInfo: "test.TestMessage", // Full message name
	}

	// Create test protobuf data
	// This represents a message with id=150 and name="test"
	testData := []byte{
		0x08, 0x96, 0x01, // field 1 (id): varint 150
		0x12, 0x04,       // field 2 (name): length-delimited, length 4
		0x74, 0x65, 0x73, 0x74, // "test"
	}

	// Test deserialization
	result, err := deserializer.Deserialize(testData, schema)
	require.NoError(t, err, "Deserialization should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// Verify the result is a dynamic message
	dynamicMsg, ok := result.(*dynamicpb.Message)
	require.True(t, ok, "Result should be a dynamic protobuf message")

	// Verify field values
	descriptor := dynamicMsg.Descriptor()
	fields := descriptor.Fields()
	
	idField := fields.ByName("id")
	require.NotNil(t, idField, "Should have id field")
	idValue := dynamicMsg.Get(idField)
	assert.Equal(t, int64(150), idValue.Int(), "ID should be 150")

	nameField := fields.ByName("name")
	require.NotNil(t, nameField, "Should have name field")
	nameValue := dynamicMsg.Get(nameField)
	assert.Equal(t, "test", nameValue.String(), "Name should be 'test'")
}

func TestProtobufDeserializer_Deserialize_WithoutAdditionalInfo(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	
	// Create a simple test message descriptor
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("SimpleMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("value"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	// Create FileDescriptorSet
	fileDescSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}

	// Serialize the descriptor set to use as schema definition
	schemaDefBytes, err := proto.Marshal(fileDescSet)
	require.NoError(t, err, "Should marshal file descriptor set")

	schema := &gsrserde.Schema{
		Name:           "SimpleMessage",
		Definition:     string(schemaDefBytes),
		DataFormat:     "PROTOBUF",
		AdditionalInfo: "", // No additional info - should auto-detect first message
	}

	// Create test protobuf data
	testData := []byte{
		0x0A, 0x05,       // field 1 (value): length-delimited, length 5
		0x68, 0x65, 0x6C, 0x6C, 0x6F, // "hello"
	}

	// Test deserialization
	result, err := deserializer.Deserialize(testData, schema)
	require.NoError(t, err, "Deserialization should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// Verify the result
	dynamicMsg, ok := result.(*dynamicpb.Message)
	require.True(t, ok, "Result should be a dynamic protobuf message")

	descriptor := dynamicMsg.Descriptor()
	fields := descriptor.Fields()
	
	valueField := fields.ByName("value")
	require.NotNil(t, valueField, "Should have value field")
	fieldValue := dynamicMsg.Get(valueField)
	assert.Equal(t, "hello", fieldValue.String(), "Value should be 'hello'")
}

func TestProtobufDeserializer_Deserialize_EmptyFileDescriptorSet(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	
	// Create empty FileDescriptorSet
	fileDescSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{},
	}

	schemaDefBytes, err := proto.Marshal(fileDescSet)
	require.NoError(t, err, "Should marshal empty file descriptor set")

	schema := &gsrserde.Schema{
		Name:       "TestMessage",
		Definition: string(schemaDefBytes),
		DataFormat: "PROTOBUF",
	}

	testData := []byte{0x08, 0x96, 0x01}

	result, err := deserializer.Deserialize(testData, schema)
	assert.Nil(t, result, "Result should be nil")
	assert.ErrorIs(t, err, ErrMessageDescriptorNotFound, "Should return message descriptor not found error")
}

func TestProtobufDeserializer_Deserialize_InvalidMessageType(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	
	// Create a file descriptor with no messages, only enums
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		EnumType: []*descriptorpb.EnumDescriptorProto{
			{
				Name: proto.String("TestEnum"),
				Value: []*descriptorpb.EnumValueDescriptorProto{
					{Name: proto.String("VALUE1"), Number: proto.Int32(0)},
				},
			},
		},
	}

	fileDescSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}

	schemaDefBytes, err := proto.Marshal(fileDescSet)
	require.NoError(t, err, "Should marshal file descriptor set")

	schema := &gsrserde.Schema{
		Name:           "TestEnum",
		Definition:     string(schemaDefBytes),
		DataFormat:     "PROTOBUF",
		AdditionalInfo: "test.TestEnum", // This is an enum, not a message
	}

	testData := []byte{0x08, 0x01}

	result, err := deserializer.Deserialize(testData, schema)
	assert.Nil(t, result, "Result should be nil")
	assert.Error(t, err, "Should return error for non-message descriptor")
	assert.Contains(t, err.Error(), "is not a message descriptor", "Error should mention non-message descriptor")
}

func TestProtobufDeserializer_Deserialize_MalformedProtobufData(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	
	// Create valid schema
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("TestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
				},
			},
		},
	}

	fileDescSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}

	schemaDefBytes, err := proto.Marshal(fileDescSet)
	require.NoError(t, err, "Should marshal file descriptor set")

	schema := &gsrserde.Schema{
		Name:           "TestMessage",
		Definition:     string(schemaDefBytes),
		DataFormat:     "PROTOBUF",
		AdditionalInfo: "test.TestMessage",
	}

	// Test with malformed protobuf data
	malformedData := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF} // Invalid varint

	result, err := deserializer.Deserialize(malformedData, schema)
	assert.Nil(t, result, "Result should be nil for malformed data")
	assert.ErrorIs(t, err, ErrDeserializationFailed, "Should return deserialization failed error")
}

func TestProtobufDeserializer_MessageDescriptorCaching(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	
	// Create schema
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("TestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
				},
			},
		},
	}

	fileDescSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}

	schemaDefBytes, err := proto.Marshal(fileDescSet)
	require.NoError(t, err, "Should marshal file descriptor set")

	schema := &gsrserde.Schema{
		Name:           "TestMessage",
		Definition:     string(schemaDefBytes),
		DataFormat:     "PROTOBUF",
		AdditionalInfo: "test.TestMessage",
	}

	testData := []byte{0x08, 0x96, 0x01} // id = 150

	// First deserialization - should create descriptor
	assert.Nil(t, deserializer.messageDescriptor, "Descriptor should be nil initially")
	
	result1, err := deserializer.Deserialize(testData, schema)
	require.NoError(t, err, "First deserialization should succeed")
	require.NotNil(t, result1, "First result should not be nil")
	assert.NotNil(t, deserializer.messageDescriptor, "Descriptor should be cached")

	// Second deserialization - should use cached descriptor
	result2, err := deserializer.Deserialize(testData, schema)
	require.NoError(t, err, "Second deserialization should succeed")
	require.NotNil(t, result2, "Second result should not be nil")
	
	// Both results should be equivalent
	msg1 := result1.(*dynamicpb.Message)
	msg2 := result2.(*dynamicpb.Message)
	assert.True(t, proto.Equal(msg1, msg2), "Both deserialized messages should be equal")
}

func TestProtobufDeserializer_WithTestHelpers(t *testing.T) {
	deserializer := NewProtobufDeserializer()
	
	// Use test helpers to generate protobuf message
	msg := test_helpers.GenerateTestProtoMessage()
	require.NotNil(t, msg, "Should generate test message")
	
	// Create a simple schema that matches our test message structure
	// Note: In a real scenario, the schema definition would come from the schema registry
	// For this test, we'll create a minimal valid schema
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("simple_test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("SimpleTest"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("value"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	fileDescSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}

	schemaDefBytes, err := proto.Marshal(fileDescSet)
	require.NoError(t, err, "Should marshal file descriptor set")

	schema := &gsrserde.Schema{
		Name:           "SimpleTest",
		Definition:     string(schemaDefBytes),
		DataFormat:     "PROTOBUF",
		AdditionalInfo: "test.SimpleTest",
	}

	// Note: The test data from GenerateTestProtoMessage might not match our simple schema
	// But we can test the deserializer flow. In practice, schema and data would be aligned.
	
	// Create compatible test data for our simple schema
	simpleTestData := []byte{
		0x0A, 0x04,       // field 1 (value): length-delimited, length 4
		0x74, 0x65, 0x73, 0x74, // "test"
	}

	result, err := deserializer.Deserialize(simpleTestData, schema)
	require.NoError(t, err, "Deserialization should succeed")
	require.NotNil(t, result, "Result should not be nil")

	dynamicMsg := result.(*dynamicpb.Message)
	valueField := dynamicMsg.Descriptor().Fields().ByName("value")
	fieldValue := dynamicMsg.Get(valueField)
	assert.Equal(t, "test", fieldValue.String(), "Value should be 'test'")
}

// BenchmarkProtobufDeserializer_Deserialize benchmarks the deserialization performance
func BenchmarkProtobufDeserializer_Deserialize(b *testing.B) {
	deserializer := NewProtobufDeserializer()
	
	// Setup test data and schema
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("TestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
					{
						Name:   proto.String("name"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	fileDescSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	}

	schemaDefBytes, _ := proto.Marshal(fileDescSet)

	schema := &gsrserde.Schema{
		Name:           "TestMessage",
		Definition:     string(schemaDefBytes),
		DataFormat:     "PROTOBUF",
		AdditionalInfo: "test.TestMessage",
	}

	testData := []byte{
		0x08, 0x96, 0x01, // id = 150
		0x12, 0x04,       // name: length 4
		0x74, 0x65, 0x73, 0x74, // "test"
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := deserializer.Deserialize(testData, schema)
		if err != nil {
			b.Fatalf("Deserialization failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result should not be nil")
		}
	}
}
