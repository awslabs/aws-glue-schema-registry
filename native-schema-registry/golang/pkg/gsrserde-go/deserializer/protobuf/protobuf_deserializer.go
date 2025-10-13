package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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
func NewProtobufDeserializer(config *common.Configuration ) (*ProtobufDeserializer, error) {
	if config == nil {
		return nil, common.ErrNilConfig
	}
	if config.ProtobufMessageDescriptor == nil {
		panic("protobuf message descriptor cannot be nil")

	}
	return &ProtobufDeserializer{
		config:            config,
		messageDescriptor: config.ProtobufMessageDescriptor,
	}, nil
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


	// Create a new dynamic message instance
	dynamicMessage := dynamicpb.NewMessage(pd.messageDescriptor)

	// Unmarshal the protobuf data
	if err := proto.Unmarshal(data, dynamicMessage); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDeserializationFailed, err)
	}

	return dynamicMessage, nil
}


