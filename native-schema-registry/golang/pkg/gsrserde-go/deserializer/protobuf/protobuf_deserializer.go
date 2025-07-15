package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
)

var (
	// ErrNilData is returned when nil data is provided
	ErrNilData = fmt.Errorf("protobuf deserializer: data cannot be nil")
	
	// ErrEmptyData is returned when empty data is provided
	ErrEmptyData = fmt.Errorf("protobuf deserializer: data cannot be empty")
	
	// ErrNilSchema is returned when nil schema is provided
	ErrNilSchema = fmt.Errorf("protobuf deserializer: schema cannot be nil")
	
	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("protobuf deserializer: invalid schema definition")
	
	// ErrSchemaNotProtobuf is returned when schema is not a protobuf schema
	ErrSchemaNotProtobuf = fmt.Errorf("protobuf deserializer: schema is not a protobuf schema")
	
	// ErrMessageDescriptorNotFound is returned when the message descriptor cannot be found
	ErrMessageDescriptorNotFound = fmt.Errorf("protobuf deserializer: message descriptor not found")
	
	// ErrDeserializationFailed is returned when protobuf deserialization fails
	ErrDeserializationFailed = fmt.Errorf("protobuf deserializer: deserialization failed")
)

// ProtobufDeserializer implements the DataFormatDeserializer interface for protobuf messages.
// It supports proto2 and proto3 without extensions or groups.
type ProtobufDeserializer struct {
	// messageDescriptor holds the protobuf message descriptor for deserialization
	messageDescriptor protoreflect.MessageDescriptor
}

// NewProtobufDeserializer creates a new ProtobufDeserializer instance.
// The schema must contain a valid protobuf schema definition.
func NewProtobufDeserializer() *ProtobufDeserializer {
	return &ProtobufDeserializer{}
}

// Deserialize takes encoded protobuf data and a schema, and returns the deserialized message.
// The schema must be a protobuf schema with a valid schema definition.
//
// Parameters:
//   data: The encoded protobuf byte data to deserialize
//   schema: The protobuf schema information needed for deserialization
//
// Returns:
//   interface{}: The deserialized protobuf message as a dynamic message
//   error: Any error that occurred during deserialization
func (pd *ProtobufDeserializer) Deserialize(data []byte, schema *gsrserde.Schema) (interface{}, error) {
	// Validate input parameters
	if data == nil {
		return nil, ErrNilData
	}
	
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	
	if schema == nil {
		return nil, ErrNilSchema
	}
	
	// Verify this is a protobuf schema
	if schema.DataFormat != "PROTOBUF" {
		return nil, ErrSchemaNotProtobuf
	}
	
	if schema.Definition == "" {
		return nil, ErrInvalidSchema
	}
	
	// Get or create message descriptor from schema
	messageDescriptor, err := pd.getMessageDescriptor(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get message descriptor: %w", err)
	}
	
	// Create a new dynamic message instance
	dynamicMessage := dynamicpb.NewMessage(messageDescriptor)
	
	// Unmarshal the protobuf data
	if err := proto.Unmarshal(data, dynamicMessage); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDeserializationFailed, err)
	}
	
	return dynamicMessage, nil
}

// getMessageDescriptor extracts or creates a message descriptor from the schema.
// It parses the protobuf schema definition and creates the appropriate descriptor.
func (pd *ProtobufDeserializer) getMessageDescriptor(schema *gsrserde.Schema) (protoreflect.MessageDescriptor, error) {
	
	// Parse the schema definition as a FileDescriptorSet
	var fileDescriptorSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal([]byte(schema.Definition), &fileDescriptorSet); err != nil {
		// If that fails, try parsing as a single FileDescriptor
		var fileDescriptor descriptorpb.FileDescriptorProto
		if err2 := proto.Unmarshal([]byte(schema.Definition), &fileDescriptor); err2 != nil {
			return nil, fmt.Errorf("failed to parse schema definition as FileDescriptorSet or FileDescriptorProto: %v, %v", err, err2)
		}
		fileDescriptorSet.File = []*descriptorpb.FileDescriptorProto{&fileDescriptor}
	}
	
	// Create file descriptors from the descriptor set
	files, err := protodesc.NewFiles(&fileDescriptorSet)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptors: %w", err)
	}
	
	// Find the message descriptor
	// Use AdditionalInfo if provided (should contain the message type name)
	var messageTypeName string
	if schema.AdditionalInfo != "" {
		messageTypeName = schema.AdditionalInfo
	} else {
		// Try to find the first message type in the first file
		files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
			if fd.Messages().Len() > 0 {
				messageTypeName = string(fd.Messages().Get(0).FullName())
				return false // Stop iteration
			}
			return true // Continue iteration
		})
		
		if messageTypeName == "" {
			return nil, ErrMessageDescriptorNotFound
		}
	}
	
	// Find the message descriptor by name
	messageDescriptor, err := files.FindDescriptorByName(protoreflect.FullName(messageTypeName))
	if err != nil {
		return nil, fmt.Errorf("failed to find message descriptor '%s': %w", messageTypeName, err)
	}
	
	// Ensure it's a message descriptor
	msgDesc, ok := messageDescriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor '%s' is not a message descriptor", messageTypeName)
	}
	
	// Cache the descriptor
	pd.messageDescriptor = msgDesc
	
	return msgDesc, nil
}
