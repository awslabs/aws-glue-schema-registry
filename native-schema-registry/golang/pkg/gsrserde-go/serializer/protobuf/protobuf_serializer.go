package protobuf

import (
	"encoding/base64"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidProtoMessage is returned when the data is not a valid protobuf message
	ErrInvalidProtoMessage = fmt.Errorf("data must be a proto.Message")

	// ErrNilMessage is returned when a nil message is provided
	ErrNilMessage = fmt.Errorf("proto message cannot be nil")

	// ErrSchemaGeneration is returned when schema generation fails
	ErrSchemaGeneration = fmt.Errorf("failed to generate schema from proto message")

	// ErrSerialization is returned when protobuf serialization fails
	ErrSerialization = fmt.Errorf("protobuf serialization failed")

	// ErrValidation is returned when protobuf validation fails
	ErrValidation = fmt.Errorf("protobuf validation failed")
)

// ProtobufSerializationError represents an error that occurred during protobuf serialization
type ProtobufSerializationError struct {
	Message string
	Cause   error
}

func (e *ProtobufSerializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("protobuf serialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("protobuf serialization error: %s", e.Message)
}

func (e *ProtobufSerializationError) Unwrap() error {
	return e.Cause
}

// ProtobufValidationError represents an error that occurred during protobuf validation
type ProtobufValidationError struct {
	Message string
	Cause   error
}

func (e *ProtobufValidationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("protobuf validation error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("protobuf validation error: %s", e.Message)
}

func (e *ProtobufValidationError) Unwrap() error {
	return e.Cause
}

// ProtobufSerializer handles serialization of protobuf messages.
// It uses the google.golang.org/protobuf library for pure Go implementation.
type ProtobufSerializer struct {
	// This serializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewProtobufSerializer creates a new protobuf serializer instance.
func NewProtobufSerializer(config *common.Configuration) *ProtobufSerializer {
	if config == nil {
		panic("configuration cannot be nil")
	}
	return &ProtobufSerializer{
		config: config,
	}
}

// Serialize serializes a protobuf message to bytes.
// The input data must implement proto.Message interface.
//
// Parameters:
//
//	data: Must be a proto.Message implementation
//
// Returns:
//
//	[]byte: The serialized protobuf data
//	error: Any error that occurred during serialization
func (p *ProtobufSerializer) Serialize(data interface{}) ([]byte, error) {
	if data == nil {
		return nil, &ProtobufSerializationError{
			Message: "cannot serialize nil data",
			Cause:   ErrNilMessage,
		}
	}

	// Check if data implements proto.Message
	protoMsg, ok := data.(proto.Message)
	if !ok {
		return nil, &ProtobufSerializationError{
			Message: fmt.Sprintf("expected proto.Message, got %T", data),
			Cause:   ErrInvalidProtoMessage,
		}
	}

	// Validate the message before serialization
	if err := p.ValidateObject(data); err != nil {
		return nil, &ProtobufSerializationError{
			Message: "message validation failed",
			Cause:   err,
		}
	}

	// Serialize using proto.Marshal
	serializedData, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, &ProtobufSerializationError{
			Message: "failed to marshal protobuf message",
			Cause:   err,
		}
	}

	return serializedData, nil
}

// GetSchemaDefinition extracts the schema definition from a protobuf message.
// It returns the FileDescriptorProto as a JSON string.
//
// Parameters:
//
//	data: Must be a proto.Message implementation
//
// Returns:
//
//	string: The schema definition as JSON
//	error: Any error that occurred during schema extraction
func (p *ProtobufSerializer) GetSchemaDefinition(data interface{}) (string, error) {
	if data == nil {
		return "", &ProtobufSerializationError{
			Message: "cannot get schema from nil data",
			Cause:   ErrNilMessage,
		}
	}

	// Check if data implements proto.Message
	protoMsg, ok := data.(proto.Message)
	if !ok {
		return "", &ProtobufSerializationError{
			Message: fmt.Sprintf("expected proto.Message, got %T", data),
			Cause:   ErrInvalidProtoMessage,
		}
	}

	// Get message descriptor
	msgDesc := protoMsg.ProtoReflect().Descriptor()
	if msgDesc == nil {
		return "", &ProtobufSerializationError{
			Message: "failed to get message descriptor",
			Cause:   ErrSchemaGeneration,
		}
	}

	// Get file descriptor
	fileDesc := msgDesc.ParentFile()
	if fileDesc == nil {
		return "", &ProtobufSerializationError{
			Message: "failed to get file descriptor",
			Cause:   ErrSchemaGeneration,
		}
	}

	// Convert to FileDescriptorProto
	fileDescProto := protodesc.ToFileDescriptorProto(fileDesc)
	if fileDescProto == nil {
		return "", &ProtobufSerializationError{
			Message: "failed to convert to FileDescriptorProto",
			Cause:   ErrSchemaGeneration,
		}
	}

	// Marshal to bytes first
	protoBytes, err := proto.Marshal(fileDescProto)
	if err != nil {
		return "", &ProtobufSerializationError{
			Message: "failed to marshal FileDescriptorProto",
			Cause:   err,
		}
	}

	// Return base64-encoded protobuf bytes as expected by the Java GSR library
	// The Java ProtobufPreprocessor.convertBase64SchemaToStringSchema() expects base64-encoded data
	return base64.StdEncoding.EncodeToString(protoBytes), nil
}

// Validate validates serialized protobuf data against a schema definition.
// This is a basic validation that attempts to unmarshal the data.
//
// Parameters:
//
//	schemaDefinition: The schema definition (currently not used for validation)
//	data: The serialized protobuf data to validate
//
// Returns:
//
//	error: Any validation error, nil if valid
func (p *ProtobufSerializer) Validate(schemaDefinition string, data []byte) error {
	if data == nil {
		return &ProtobufValidationError{
			Message: "data cannot be nil",
			Cause:   ErrNilMessage,
		}
	}

	if len(data) == 0 {
		return &ProtobufValidationError{
			Message: "data cannot be empty",
			Cause:   ErrValidation,
		}
	}

	// For now, we perform basic validation by checking if the data
	// appears to be valid protobuf format. A more sophisticated
	// implementation would parse the schema definition and validate
	// against the specific message type.

	// Basic protobuf format validation - check if it starts with valid varint
	// This is a simplified check and may not catch all invalid protobuf data
	if data[0] > 0x80 && len(data) < 2 {
		return &ProtobufValidationError{
			Message: "invalid protobuf format: incomplete varint",
			Cause:   ErrValidation,
		}
	}

	return nil
}

// ValidateObject validates a protobuf message object.
// It checks if the object is a valid proto.Message and performs basic validation.
//
// Parameters:
//
//	data: The protobuf message object to validate
//
// Returns:
//
//	error: Any validation error, nil if valid
func (p *ProtobufSerializer) ValidateObject(data interface{}) error {
	if data == nil {
		return &ProtobufValidationError{
			Message: "message cannot be nil",
			Cause:   ErrNilMessage,
		}
	}

	// Check if data implements proto.Message
	protoMsg, ok := data.(proto.Message)
	if !ok {
		return &ProtobufValidationError{
			Message: fmt.Sprintf("expected proto.Message, got %T", data),
			Cause:   ErrInvalidProtoMessage,
		}
	}

	// Check if the message is valid by getting its reflection
	reflection := protoMsg.ProtoReflect()
	if reflection == nil {
		return &ProtobufValidationError{
			Message: "failed to get proto reflection",
			Cause:   ErrValidation,
		}
	}

	// Check if the message descriptor is valid
	msgDesc := reflection.Descriptor()
	if msgDesc == nil {
		return &ProtobufValidationError{
			Message: "failed to get message descriptor",
			Cause:   ErrValidation,
		}
	}

	// Additional validation could be added here, such as:
	// - Required field validation
	// - Field type validation
	// - Custom validation rules

	return nil
}

// SetAdditionalSchemaInfo sets additional schema information in the schema object.
// For protobuf, this typically includes the message type name.
//
// Parameters:
//
//	data: The protobuf message object
//	schema: The schema object to update
//
// Returns:
//
//	error: Any error that occurred during schema update
func (p *ProtobufSerializer) SetAdditionalSchemaInfo(data interface{}, schema *gsrserde.Schema) error {
	if data == nil {
		return &ProtobufSerializationError{
			Message: "data cannot be nil",
			Cause:   ErrNilMessage,
		}
	}

	if schema == nil {
		return &ProtobufSerializationError{
			Message: "schema cannot be nil",
			Cause:   fmt.Errorf("schema is nil"),
		}
	}

	// Check if data implements proto.Message
	protoMsg, ok := data.(proto.Message)
	if !ok {
		return &ProtobufSerializationError{
			Message: fmt.Sprintf("expected proto.Message, got %T", data),
			Cause:   ErrInvalidProtoMessage,
		}
	}

	// Get the message type name
	msgDesc := protoMsg.ProtoReflect().Descriptor()
	if msgDesc != nil {
		// Set the full message name as additional info
		schema.AdditionalInfo = string(msgDesc.FullName())
	} else {
		// Fall back to the Go type name
		schema.AdditionalInfo = reflect.TypeOf(data).String()
	}

	// Ensure DataFormat is set correctly
	if schema.DataFormat == "" {
		schema.DataFormat = "PROTOBUF"
	}

	return nil
}
