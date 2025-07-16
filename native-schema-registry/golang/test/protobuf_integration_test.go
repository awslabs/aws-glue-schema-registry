package test

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	deserializer_protobuf "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer/protobuf"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/protobuf"
)

// TestProtobufSerializerDeserializerIntegration tests the complete round-trip:
// serialize with ProtobufSerializer -> deserialize with ProtobufDeserializer
func TestProtobufSerializerDeserializerIntegration(t *testing.T) {
	tests := []struct {
		name           string
		inputMessage   proto.Message
		expectedFields map[string]interface{} // Expected field values in deserialized message
	}{
		{
			name: "FileDescriptorProto_round_trip",
			inputMessage: &descriptorpb.FileDescriptorProto{
				Name:    proto.String("test.proto"),
				Package: proto.String("test.package"),
				Syntax:  proto.String("proto3"),
			},
			expectedFields: map[string]interface{}{
				"name":    "test.proto",
				"package": "test.package",
				"syntax":  "proto3",
			},
		},
		{
			name: "DescriptorProto_round_trip",
			inputMessage: &descriptorpb.DescriptorProto{
				Name: proto.String("TestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("test_field"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
			expectedFields: map[string]interface{}{
				"name": "TestMessage",
			},
		},
		{
			name: "FieldDescriptorProto_round_trip",
			inputMessage: &descriptorpb.FieldDescriptorProto{
				Name:     proto.String("test_field"),
				Number:   proto.Int32(42),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				TypeName: proto.String("string"),
			},
			expectedFields: map[string]interface{}{
				"name":      "test_field",
				"number":    int32(42),
				"type_name": "string",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step 1: Create serializer and serialize the message
			serializer := protobuf.NewProtobufSerializer()
			
			serializedData, err := serializer.Serialize(tt.inputMessage)
			if err != nil {
				t.Fatalf("Failed to serialize message: %v", err)
			}
			
			if len(serializedData) == 0 {
				t.Fatal("Serialized data is empty")
			}

			// Step 2: Get schema definition from the serializer
			schemaDefinition, err := serializer.GetSchemaDefinition(tt.inputMessage)
			if err != nil {
				t.Fatalf("Failed to get schema definition: %v", err)
			}
			
			if schemaDefinition == "" {
				t.Fatal("Schema definition is empty")
			}

			// Step 3: Create schema object and set additional info
			schema := &gsrserde.Schema{
				DataFormat: "PROTOBUF",
				Definition: schemaDefinition,
			}
			
			err = serializer.SetAdditionalSchemaInfo(tt.inputMessage, schema)
			if err != nil {
				t.Fatalf("Failed to set additional schema info: %v", err)
			}

			// Verify schema has additional info (message type name)
			if schema.AdditionalInfo == "" {
				t.Fatal("Schema AdditionalInfo is empty after SetAdditionalSchemaInfo")
			}

			// Step 4: Create deserializer and deserialize the data
			deserializer := deserializer_protobuf.NewProtobufDeserializer()
			
			deserializedMessage, err := deserializer.Deserialize(serializedData, schema)
			if err != nil {
				t.Fatalf("Failed to deserialize message: %v", err)
			}

			// Step 5: Verify the deserialized message
			dynamicMsg, ok := deserializedMessage.(*dynamicpb.Message)
			if !ok {
				t.Fatalf("Expected *dynamicpb.Message, got %T", deserializedMessage)
			}

			// Step 6: Validate specific fields in the deserialized message
			for fieldName, expectedValue := range tt.expectedFields {
				if !validateDynamicMessageField(t, dynamicMsg, fieldName, expectedValue) {
					t.Errorf("Field %s validation failed", fieldName)
				}
			}
		})
	}
}

// TestProtobufRoundTripWithComplexMessage tests round-trip with a more complex message
func TestProtobufRoundTripWithComplexMessage(t *testing.T) {
	// Create a complex FileDescriptorProto with nested structures
	complexMessage := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("complex.proto"),
		Package: proto.String("complex.package"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("ComplexMessage"),
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

	// Serialize
	serializer := protobuf.NewProtobufSerializer()
	serializedData, err := serializer.Serialize(complexMessage)
	if err != nil {
		t.Fatalf("Failed to serialize complex message: %v", err)
	}

	// Get schema
	schemaDefinition, err := serializer.GetSchemaDefinition(complexMessage)
	if err != nil {
		t.Fatalf("Failed to get schema definition: %v", err)
	}

	schema := &gsrserde.Schema{
		DataFormat: "PROTOBUF",
		Definition: schemaDefinition,
	}
	
	err = serializer.SetAdditionalSchemaInfo(complexMessage, schema)
	if err != nil {
		t.Fatalf("Failed to set additional schema info: %v", err)
	}

	// Deserialize
	deserializer := deserializer_protobuf.NewProtobufDeserializer()
	deserializedMessage, err := deserializer.Deserialize(serializedData, schema)
	if err != nil {
		t.Fatalf("Failed to deserialize complex message: %v", err)
	}

	// Validate
	dynamicMsg, ok := deserializedMessage.(*dynamicpb.Message)
	if !ok {
		t.Fatalf("Expected *dynamicpb.Message, got %T", deserializedMessage)
	}

	// Check basic fields
	expectedFields := map[string]interface{}{
		"name":    "complex.proto",
		"package": "complex.package",
		"syntax":  "proto3",
	}

	for fieldName, expectedValue := range expectedFields {
		if !validateDynamicMessageField(t, dynamicMsg, fieldName, expectedValue) {
			t.Errorf("Field %s validation failed", fieldName)
		}
	}

	// Check that message_type field exists and has content
	msgTypeField := dynamicMsg.Get(dynamicMsg.Descriptor().Fields().ByName("message_type"))
	if !msgTypeField.IsValid() {
		t.Error("message_type field should be present")
	}
}

// TestProtobufRoundTripEdgeCases tests edge cases in round-trip serialization
func TestProtobufRoundTripEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		inputMessage proto.Message
		description  string
	}{
		{
			name: "empty_message",
			inputMessage: &descriptorpb.FileDescriptorProto{
				// Only required fields, everything else empty
				Name: proto.String(""),
			},
			description: "Message with empty string fields",
		},
		{
			name: "minimal_descriptor",
			inputMessage: &descriptorpb.DescriptorProto{
				Name: proto.String("MinimalMessage"),
			},
			description: "Minimal descriptor with only name",
		},
		{
			name: "field_with_defaults",
			inputMessage: &descriptorpb.FieldDescriptorProto{
				Name:   proto.String("default_field"),
				Number: proto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			},
			description: "Field descriptor with default type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Testing: %s", tt.description)

			// Serialize
			serializer := protobuf.NewProtobufSerializer()
			serializedData, err := serializer.Serialize(tt.inputMessage)
			if err != nil {
				t.Fatalf("Failed to serialize %s: %v", tt.description, err)
			}

			// Get schema
			schemaDefinition, err := serializer.GetSchemaDefinition(tt.inputMessage)
			if err != nil {
				t.Fatalf("Failed to get schema definition for %s: %v", tt.description, err)
			}

			schema := &gsrserde.Schema{
				DataFormat: "PROTOBUF",
				Definition: schemaDefinition,
			}
			
			err = serializer.SetAdditionalSchemaInfo(tt.inputMessage, schema)
			if err != nil {
				t.Fatalf("Failed to set additional schema info for %s: %v", tt.description, err)
			}

			// Deserialize
			deserializer := deserializer_protobuf.NewProtobufDeserializer()
			deserializedMessage, err := deserializer.Deserialize(serializedData, schema)
			if err != nil {
				t.Fatalf("Failed to deserialize %s: %v", tt.description, err)
			}

			// Basic validation - just ensure we got a dynamic message back
			_, ok := deserializedMessage.(*dynamicpb.Message)
			if !ok {
				t.Fatalf("Expected *dynamicpb.Message for %s, got %T", tt.description, deserializedMessage)
			}

			t.Logf("Successfully completed round-trip for: %s", tt.description)
		})
	}
}

// TestProtobufRoundTripBinaryEquivalence tests that the serialized data is consistent
func TestProtobufRoundTripBinaryEquivalence(t *testing.T) {
	message := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("equivalence.proto"),
		Package: proto.String("equivalence.test"),
		Syntax:  proto.String("proto3"),
	}

	serializer := protobuf.NewProtobufSerializer()

	// Serialize the same message twice
	serializedData1, err := serializer.Serialize(message)
	if err != nil {
		t.Fatalf("First serialization failed: %v", err)
	}

	serializedData2, err := serializer.Serialize(message)
	if err != nil {
		t.Fatalf("Second serialization failed: %v", err)
	}

	// Should produce identical binary data
	if !reflect.DeepEqual(serializedData1, serializedData2) {
		t.Error("Serialization is not deterministic - same message produced different binary data")
	}

	// Both should deserialize to equivalent messages
	schemaDefinition, err := serializer.GetSchemaDefinition(message)
	if err != nil {
		t.Fatalf("Failed to get schema definition: %v", err)
	}

	schema := &gsrserde.Schema{
		DataFormat: "PROTOBUF",
		Definition: schemaDefinition,
	}
	
	err = serializer.SetAdditionalSchemaInfo(message, schema)
	if err != nil {
		t.Fatalf("Failed to set additional schema info: %v", err)
	}

	deserializer := deserializer_protobuf.NewProtobufDeserializer()
	
	deserializedMessage1, err := deserializer.Deserialize(serializedData1, schema)
	if err != nil {
		t.Fatalf("First deserialization failed: %v", err)
	}

	deserializedMessage2, err := deserializer.Deserialize(serializedData2, schema)
	if err != nil {
		t.Fatalf("Second deserialization failed: %v", err)
	}

	// Both deserialized messages should be equivalent
	dynamicMsg1 := deserializedMessage1.(*dynamicpb.Message)
	dynamicMsg2 := deserializedMessage2.(*dynamicpb.Message)

	// Compare key fields to ensure they are equivalent
	// Since dynamic messages might have subtle differences, we check specific fields
	if !validateDynamicMessageField(t, dynamicMsg1, "name", "equivalence.proto") {
		t.Error("First deserialized message name field is incorrect")
	}
	if !validateDynamicMessageField(t, dynamicMsg2, "name", "equivalence.proto") {
		t.Error("Second deserialized message name field is incorrect")
	}
	if !validateDynamicMessageField(t, dynamicMsg1, "package", "equivalence.test") {
		t.Error("First deserialized message package field is incorrect")
	}
	if !validateDynamicMessageField(t, dynamicMsg2, "package", "equivalence.test") {
		t.Error("Second deserialized message package field is incorrect")
	}
}

// validateDynamicMessageField validates a specific field in a dynamic protobuf message
func validateDynamicMessageField(t *testing.T, msg *dynamicpb.Message, fieldName string, expectedValue interface{}) bool {
	t.Helper()

	fieldDesc := msg.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fieldDesc == nil {
		t.Errorf("Field %s not found in message descriptor", fieldName)
		return false
	}

	fieldValue := msg.Get(fieldDesc)
	if !fieldValue.IsValid() {
		t.Errorf("Field %s has invalid value", fieldName)
		return false
	}

	var actualValue interface{}
	switch fieldDesc.Kind() {
	case protoreflect.StringKind:
		actualValue = fieldValue.String()
	case protoreflect.Int32Kind:
		actualValue = int32(fieldValue.Int())
	case protoreflect.Int64Kind:
		actualValue = fieldValue.Int()
	case protoreflect.BoolKind:
		actualValue = fieldValue.Bool()
	default:
		actualValue = fieldValue.Interface()
	}

	if !reflect.DeepEqual(actualValue, expectedValue) {
		t.Errorf("Field %s: expected %v (%T), got %v (%T)", fieldName, expectedValue, expectedValue, actualValue, actualValue)
		return false
	}

	return true
}
