package test_helpers

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// TestMessage represents a simple test message for protobuf testing
type TestMessage struct {
	ID      int64
	Name    string
	Active  bool
	Created time.Time
}

// GenerateTestProtoMessage creates a simple protobuf message for testing
func GenerateTestProtoMessage() proto.Message {
	// Create a simple descriptor for testing
	desc := &descriptorpb.FileDescriptorProto{
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
					{
						Name:   proto.String("active"),
						Number: proto.Int32(3),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
					},
				},
			},
		},
	}

	// Create file descriptor
	fileDesc, err := protodesc.NewFile(desc, protoregistry.GlobalFiles)
	if err != nil {
		// If we can't create a dynamic message, return a simple one
		return createSimpleTestMessage()
	}

	// Get message descriptor
	msgDesc := fileDesc.Messages().ByName("TestMessage")
	if msgDesc == nil {
		return createSimpleTestMessage()
	}

	// Create dynamic message
	msg := dynamicpb.NewMessage(msgDesc)
	
	// Set field values
	msg.Set(msgDesc.Fields().ByName("id"), protoreflect.ValueOfInt64(12345))
	msg.Set(msgDesc.Fields().ByName("name"), protoreflect.ValueOfString("test-message"))
	msg.Set(msgDesc.Fields().ByName("active"), protoreflect.ValueOfBool(true))

	return msg
}

// createSimpleTestMessage creates a simple test message without dynamic protobuf
func createSimpleTestMessage() proto.Message {
	// For simplicity, we'll create a basic message using descriptorpb
	return &descriptorpb.FileDescriptorProto{
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
}

// GenerateTestProtoMessages creates multiple test messages
func GenerateTestProtoMessages(count int) []proto.Message {
	messages := make([]proto.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = GenerateTestProtoMessage()
	}
	return messages
}

// GenerateSerializedTestData creates serialized test data of specified size
func GenerateSerializedTestData(size int) []byte {
	if size <= 0 {
		return []byte{}
	}

	// Create a test message and serialize it
	msg := GenerateTestProtoMessage()
	data, err := proto.Marshal(msg)
	if err != nil {
		// Fallback to simple byte array
		data = make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}
		return data
	}

	// If serialized data is smaller than requested size, repeat it
	if len(data) < size {
		result := make([]byte, size)
		for i := 0; i < size; i++ {
			result[i] = data[i%len(data)]
		}
		return result
	}

	// If serialized data is larger, truncate it
	return data[:size]
}

// ValidateProtoMessage checks if a byte array contains valid protobuf data
func ValidateProtoMessage(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	// Try to unmarshal as a generic protobuf message
	msg := GenerateTestProtoMessage()
	err := proto.Unmarshal(data, msg)
	if err != nil {
		return fmt.Errorf("invalid protobuf data: %v", err)
	}

	return nil
}

// CreateTestProtoSchema generates a test protobuf schema definition
func CreateTestProtoSchema() string {
	return `
syntax = "proto3";

package test;

message TestMessage {
  int64 id = 1;
  string name = 2;
  bool active = 3;
  repeated string tags = 4;
  
  message NestedMessage {
    string value = 1;
    int32 count = 2;
  }
  
  NestedMessage nested = 5;
}

message AnotherMessage {
  string description = 1;
  double score = 2;
}
`
}

// GenerateComplexTestMessage creates a more complex test message
func GenerateComplexTestMessage(id int64, name string) proto.Message {
	// Create descriptor for a more complex message
	desc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("complex_test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("ComplexTestMessage"),
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
					{
						Name:   proto.String("score"),
						Number: proto.Int32(3),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum(),
					},
					{
						Name:   proto.String("tags"),
						Number: proto.Int32(4),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
					},
				},
			},
		},
	}

	// Try to create dynamic message, fallback to simple if it fails
	fileDesc, err := protodesc.NewFile(desc, protoregistry.GlobalFiles)
	if err != nil {
		return createSimpleTestMessage()
	}

	msgDesc := fileDesc.Messages().ByName("ComplexTestMessage")
	if msgDesc == nil {
		return createSimpleTestMessage()
	}

	msg := dynamicpb.NewMessage(msgDesc)
	
	// Set values with provided parameters
	msg.Set(msgDesc.Fields().ByName("id"), protoreflect.ValueOfInt64(id))
	msg.Set(msgDesc.Fields().ByName("name"), protoreflect.ValueOfString(name))
	msg.Set(msgDesc.Fields().ByName("score"), protoreflect.ValueOfFloat64(99.5))

	return msg
}
