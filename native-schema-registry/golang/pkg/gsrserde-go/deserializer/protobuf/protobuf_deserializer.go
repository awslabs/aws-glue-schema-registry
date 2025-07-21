package protobuf

import (
	"encoding/base64"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
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
	// config holds the configuration for the deserializer
	config *common.Configuration
}

// NewProtobufDeserializer creates a new ProtobufDeserializer instance.
// The configuration must contain the protobuf message descriptor for deserialization.
func NewProtobufDeserializer(config *common.Configuration) *ProtobufDeserializer {
	if config == nil {
		panic("configuration cannot be nil")
	}
	return &ProtobufDeserializer{
		config:            config,
		messageDescriptor: config.ProtobufMessageDescriptor(),
	}
}

// Deserialize takes encoded protobuf data and a schema, and returns the deserialized message.
// The schema must be a protobuf schema with a valid schema definition.
//
// Parameters:
//
//	data: The encoded protobuf byte data to deserialize
//	schema: The protobuf schema information needed for deserialization
//
// Returns:
//
//	interface{}: The deserialized protobuf message as a dynamic message
//	error: Any error that occurred during deserialization
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

	var fileDescriptorSet descriptorpb.FileDescriptorSet
	var err error

	// Check if the schema definition is text format (.proto file content)
	if strings.Contains(schema.Definition, "syntax") && strings.Contains(schema.Definition, "message") {
		// This is a text format .proto file from Java GSR
		// We need to parse it and create a FileDescriptorProto
		return pd.parseTextFormatSchema(schema)
	}

	var schemaBytes []byte

	// Try to decode as base64 first (Go serializer output format)
	schemaBytes, err = base64.StdEncoding.DecodeString(schema.Definition)
	if err != nil {
		// If base64 decoding fails, treat as raw bytes (test format)
		schemaBytes = []byte(schema.Definition)
	}

	// Parse the schema definition as a FileDescriptorSet
	if err := proto.Unmarshal(schemaBytes, &fileDescriptorSet); err != nil {
		// If that fails, try parsing as a single FileDescriptor
		var fileDescriptor descriptorpb.FileDescriptorProto
		if err2 := proto.Unmarshal(schemaBytes, &fileDescriptor); err2 != nil {
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

// parseTextFormatSchema parses a text format .proto schema definition and creates a message descriptor.
// This is a simplified parser that handles basic .proto syntax to create FileDescriptorProto.
func (pd *ProtobufDeserializer) parseTextFormatSchema(schema *gsrserde.Schema) (protoreflect.MessageDescriptor, error) {
	// For compatibility with Java GSR, we need to parse text format .proto files
	// Since Go doesn't have a built-in .proto text parser, we'll implement a basic one

	// Extract package name
	packageName := extractPackageName(schema.Definition)

	// Extract message definitions
	messages, err := extractMessageDefinitions(schema.Definition)
	if err != nil {
		return nil, fmt.Errorf("failed to extract message definitions: %w", err)
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("no message definitions found in schema")
	}

	// Create FileDescriptorProto
	fileDescriptor := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("generated.proto"),
		Package: proto.String(packageName),
		Syntax:  proto.String("proto3"), // Default to proto3
	}

	// Add message types
	for _, msg := range messages {
		fileDescriptor.MessageType = append(fileDescriptor.MessageType, msg)
	}

	// Create FileDescriptorSet
	fileDescriptorSet := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDescriptor},
	}

	// Create file descriptors from the descriptor set
	files, err := protodesc.NewFiles(fileDescriptorSet)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptors: %w", err)
	}

	// Find the message descriptor
	var messageTypeName string
	if schema.AdditionalInfo != "" {
		messageTypeName = schema.AdditionalInfo
	} else {
		// Use the first message type
		if len(messages) > 0 && messages[0].Name != nil {
			if packageName != "" {
				messageTypeName = packageName + "." + *messages[0].Name
			} else {
				messageTypeName = *messages[0].Name
			}
		}
	}

	if messageTypeName == "" {
		return nil, ErrMessageDescriptorNotFound
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

// extractPackageName extracts the package name from a .proto file content
func extractPackageName(protoContent string) string {
	lines := strings.Split(protoContent, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "package ") {
			// Extract package name between "package " and ";"
			packageLine := strings.TrimPrefix(line, "package ")
			packageLine = strings.TrimSuffix(packageLine, ";")
			return strings.TrimSpace(packageLine)
		}
	}
	return ""
}

// extractMessageDefinitions extracts message definitions from a .proto file content
func extractMessageDefinitions(protoContent string) ([]*descriptorpb.DescriptorProto, error) {
	var messages []*descriptorpb.DescriptorProto

	lines := strings.Split(protoContent, "\n")
	var currentMessage *descriptorpb.DescriptorProto
	var braceCount int
	var inMessage bool

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip comments and empty lines
		if strings.HasPrefix(line, "//") || strings.HasPrefix(line, "/*") || line == "" {
			continue
		}

		// Check for message start
		if strings.HasPrefix(line, "message ") && !inMessage {
			// Extract message name
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				messageName := parts[1]
				// Remove any trailing '{' from the name
				messageName = strings.TrimSuffix(messageName, "{")

				currentMessage = &descriptorpb.DescriptorProto{
					Name: proto.String(messageName),
				}
				inMessage = true
				braceCount = 0

				// Count opening braces on this line
				braceCount += strings.Count(line, "{")
				braceCount -= strings.Count(line, "}")
			}
			continue
		}

		if inMessage {
			// Count braces to track nesting
			braceCount += strings.Count(line, "{")
			braceCount -= strings.Count(line, "}")

			// Parse field definitions
			if strings.Contains(line, "=") && !strings.HasPrefix(line, "message") && !strings.HasPrefix(line, "enum") {
				field, err := parseFieldDefinition(line, int32(len(currentMessage.Field)+1))
				if err == nil && field != nil {
					currentMessage.Field = append(currentMessage.Field, field)
				}
			}

			// Check if message is complete
			if braceCount <= 0 {
				inMessage = false
				if currentMessage != nil {
					messages = append(messages, currentMessage)
					currentMessage = nil
				}
			}
		}
	}

	// Handle case where message doesn't end properly
	if currentMessage != nil {
		messages = append(messages, currentMessage)
	}

	return messages, nil
}

// parseFieldDefinition parses a field definition line and returns a FieldDescriptorProto
func parseFieldDefinition(line string, fieldNumber int32) (*descriptorpb.FieldDescriptorProto, error) {
	// Remove semicolon and extra whitespace
	line = strings.TrimSuffix(strings.TrimSpace(line), ";")

	// Split by '=' to separate field definition from field number
	parts := strings.Split(line, "=")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid field definition: %s", line)
	}

	// Parse field number
	fieldNumStr := strings.TrimSpace(parts[1])
	var actualFieldNumber int32
	if _, err := fmt.Sscanf(fieldNumStr, "%d", &actualFieldNumber); err != nil {
		return nil, fmt.Errorf("invalid field number: %s", fieldNumStr)
	}

	// Parse field definition (type and name)
	fieldDef := strings.TrimSpace(parts[0])
	fieldParts := strings.Fields(fieldDef)

	if len(fieldParts) < 2 {
		return nil, fmt.Errorf("invalid field definition: %s", fieldDef)
	}

	var fieldType string
	var fieldName string
	var label descriptorpb.FieldDescriptorProto_Label

	// Handle repeated fields
	if fieldParts[0] == "repeated" {
		if len(fieldParts) < 3 {
			return nil, fmt.Errorf("invalid repeated field definition: %s", fieldDef)
		}
		label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		fieldType = fieldParts[1]
		fieldName = fieldParts[2]
	} else {
		fieldType = fieldParts[0]
		fieldName = fieldParts[1]
		label = descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	}

	// Map protobuf types to FieldDescriptorProto types
	protoType := mapProtoType(fieldType)

	field := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(fieldName),
		Number: proto.Int32(actualFieldNumber),
		Type:   protoType.Enum(),
		Label:  label.Enum(),
	}

	// Set type name for message types
	if protoType == descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		field.TypeName = proto.String(fieldType)
	}

	return field, nil
}

// mapProtoType maps protobuf type strings to FieldDescriptorProto_Type
func mapProtoType(protoType string) descriptorpb.FieldDescriptorProto_Type {
	switch protoType {
	case "double":
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	case "float":
		return descriptorpb.FieldDescriptorProto_TYPE_FLOAT
	case "int64":
		return descriptorpb.FieldDescriptorProto_TYPE_INT64
	case "uint64":
		return descriptorpb.FieldDescriptorProto_TYPE_UINT64
	case "int32":
		return descriptorpb.FieldDescriptorProto_TYPE_INT32
	case "fixed64":
		return descriptorpb.FieldDescriptorProto_TYPE_FIXED64
	case "fixed32":
		return descriptorpb.FieldDescriptorProto_TYPE_FIXED32
	case "bool":
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL
	case "string":
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	case "bytes":
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES
	case "uint32":
		return descriptorpb.FieldDescriptorProto_TYPE_UINT32
	case "sfixed32":
		return descriptorpb.FieldDescriptorProto_TYPE_SFIXED32
	case "sfixed64":
		return descriptorpb.FieldDescriptorProto_TYPE_SFIXED64
	case "sint32":
		return descriptorpb.FieldDescriptorProto_TYPE_SINT32
	case "sint64":
		return descriptorpb.FieldDescriptorProto_TYPE_SINT64
	default:
		// Assume it's a message type
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	}
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
