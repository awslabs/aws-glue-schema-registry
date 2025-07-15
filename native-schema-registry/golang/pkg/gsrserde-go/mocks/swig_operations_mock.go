package mocks

import (
	"github.com/stretchr/testify/mock"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/GsrSerDe"
)

// MockSwigOperations is a mock implementation of SwigOperations interface
// It uses testify/mock to provide controllable behavior during testing
type MockSwigOperations struct {
	mock.Mock
}

// NewMockSwigOperations creates a new mock SWIG operations instance
func NewMockSwigOperations() *MockSwigOperations {
	return &MockSwigOperations{}
}

// Serializer operations
func (m *MockSwigOperations) NewSerializer(err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_serializer {
	args := m.Called(err)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GsrSerDe.Glue_schema_registry_serializer)
}

func (m *MockSwigOperations) DeleteSerializer(serializer GsrSerDe.Glue_schema_registry_serializer) {
	m.Called(serializer)
}

func (m *MockSwigOperations) SerializerEncode(serializer GsrSerDe.Glue_schema_registry_serializer, data GsrSerDe.Read_only_byte_array, transportName string, schema GsrSerDe.Glue_schema_registry_schema, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Mutable_byte_array {
	args := m.Called(serializer, data, transportName, schema, err)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GsrSerDe.Mutable_byte_array)
}

// Deserializer operations
func (m *MockSwigOperations) NewDeserializer(err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_deserializer {
	args := m.Called(err)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GsrSerDe.Glue_schema_registry_deserializer)
}

func (m *MockSwigOperations) DeleteDeserializer(deserializer GsrSerDe.Glue_schema_registry_deserializer) {
	m.Called(deserializer)
}

func (m *MockSwigOperations) DeserializerDecode(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Mutable_byte_array {
	args := m.Called(deserializer, data, err)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GsrSerDe.Mutable_byte_array)
}

func (m *MockSwigOperations) DeserializerCanDecode(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) bool {
	args := m.Called(deserializer, data, err)
	return args.Bool(0)
}

func (m *MockSwigOperations) DeserializerDecodeSchema(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_schema {
	args := m.Called(deserializer, data, err)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GsrSerDe.Glue_schema_registry_schema)
}

// Schema operations
func (m *MockSwigOperations) NewSchema(name, definition, dataFormat, additionalInfo string, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_schema {
	args := m.Called(name, definition, dataFormat, additionalInfo, err)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GsrSerDe.Glue_schema_registry_schema)
}

func (m *MockSwigOperations) DeleteSchema(schema GsrSerDe.Glue_schema_registry_schema) {
	m.Called(schema)
}

// Memory operations
func (m *MockSwigOperations) NewReadOnlyByteArray(dataPtr *byte, length int64, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Read_only_byte_array {
	args := m.Called(dataPtr, length, err)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GsrSerDe.Read_only_byte_array)
}

func (m *MockSwigOperations) DeleteReadOnlyByteArray(roba GsrSerDe.Read_only_byte_array) {
	m.Called(roba)
}

func (m *MockSwigOperations) DeleteMutableByteArray(mba GsrSerDe.Mutable_byte_array) {
	m.Called(mba)
}

// Error operations
func (m *MockSwigOperations) CreateErrorHolder() GsrSerDe.Glue_schema_registry_error {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GsrSerDe.Glue_schema_registry_error)
}
