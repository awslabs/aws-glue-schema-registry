package mocks

import "fmt"

// SimpleMockSwigOperations provides a simple mock implementation that doesn't require SWIG types
// This allows tests to run without C compilation
type SimpleMockSwigOperations struct {
	// Mock behavior configuration
	SerializerShouldFail   bool
	DeserializerShouldFail bool
	SchemaShouldFail       bool
	
	// Mock data
	MockSerializedData []byte
	MockDecodedData    []byte
	MockSchemaData     *MockSchema
	
	// Call tracking
	NewSerializerCalls      int
	NewDeserializerCalls    int
	NewSchemaCalls          int
	SerializerEncodeCalls   int
	DeserializerDecodeCalls int
}

// NewSimpleMockSwigOperations creates a new simple mock
func NewSimpleMockSwigOperations() *SimpleMockSwigOperations {
	return &SimpleMockSwigOperations{
		MockSerializedData: []byte("mock-serialized-data"),
		MockDecodedData:    []byte("mock-decoded-data"),
		MockSchemaData: NewMockSchema(
			"test-schema",
			"test-definition", 
			"PROTOBUF",
			"test-additional-info",
		),
	}
}

// NewSerializer mocks serializer creation
func (m *SimpleMockSwigOperations) NewSerializer(err interface{}) interface{} {
	m.NewSerializerCalls++
	if m.SerializerShouldFail {
		return nil
	}
	return NewMockSerializer("test-serializer")
}

// DeleteSerializer mocks serializer deletion
func (m *SimpleMockSwigOperations) DeleteSerializer(serializer interface{}) {
	// Mock cleanup - no-op
}

// SerializerEncode mocks encoding operation
func (m *SimpleMockSwigOperations) SerializerEncode(serializer interface{}, data interface{}, transportName string, schema interface{}, err interface{}) interface{} {
	m.SerializerEncodeCalls++
	if m.SerializerShouldFail {
		return nil
	}
	return NewMockMutableByteArray(m.MockSerializedData)
}

// NewDeserializer mocks deserializer creation
func (m *SimpleMockSwigOperations) NewDeserializer(err interface{}) interface{} {
	m.NewDeserializerCalls++
	if m.DeserializerShouldFail {
		return nil
	}
	return NewMockDeserializer("test-deserializer")
}

// DeleteDeserializer mocks deserializer deletion
func (m *SimpleMockSwigOperations) DeleteDeserializer(deserializer interface{}) {
	// Mock cleanup - no-op
}

// DeserializerDecode mocks decoding operation
func (m *SimpleMockSwigOperations) DeserializerDecode(deserializer interface{}, data interface{}, err interface{}) interface{} {
	m.DeserializerDecodeCalls++
	if m.DeserializerShouldFail {
		return nil
	}
	return NewMockMutableByteArray(m.MockDecodedData)
}

// DeserializerCanDecode mocks can decode check
func (m *SimpleMockSwigOperations) DeserializerCanDecode(deserializer interface{}, data interface{}, err interface{}) bool {
	return !m.DeserializerShouldFail
}

// DeserializerDecodeSchema mocks schema decoding
func (m *SimpleMockSwigOperations) DeserializerDecodeSchema(deserializer interface{}, data interface{}, err interface{}) interface{} {
	if m.SchemaShouldFail {
		return nil
	}
	return m.MockSchemaData
}

// NewSchema mocks schema creation
func (m *SimpleMockSwigOperations) NewSchema(name, definition, dataFormat, additionalInfo string, err interface{}) interface{} {
	m.NewSchemaCalls++
	if m.SchemaShouldFail {
		return nil
	}
	return NewMockSchema(name, definition, dataFormat, additionalInfo)
}

// DeleteSchema mocks schema deletion
func (m *SimpleMockSwigOperations) DeleteSchema(schema interface{}) {
	// Mock cleanup - no-op
}

// NewReadOnlyByteArray mocks read-only byte array creation
func (m *SimpleMockSwigOperations) NewReadOnlyByteArray(dataPtr *byte, length int64, err interface{}) interface{} {
	if dataPtr == nil || length <= 0 {
		return nil
	}
	// We can't safely access the data pointed to by dataPtr in a mock,
	// so we'll just return a mock object
	return NewMockReadOnlyByteArray([]byte("mock-data"))
}

// DeleteReadOnlyByteArray mocks read-only byte array deletion
func (m *SimpleMockSwigOperations) DeleteReadOnlyByteArray(roba interface{}) {
	// Mock cleanup - no-op
}

// DeleteMutableByteArray mocks mutable byte array deletion
func (m *SimpleMockSwigOperations) DeleteMutableByteArray(mba interface{}) {
	// Mock cleanup - no-op
}

// CreateErrorHolder mocks error holder creation
func (m *SimpleMockSwigOperations) CreateErrorHolder() interface{} {
	return nil // No error by default
}

// SetFailureMode configures the mock to simulate failures
func (m *SimpleMockSwigOperations) SetFailureMode(serializer, deserializer, schema bool) {
	m.SerializerShouldFail = serializer
	m.DeserializerShouldFail = deserializer
	m.SchemaShouldFail = schema
}

// SetMockData configures the mock data returned by operations
func (m *SimpleMockSwigOperations) SetMockData(serializedData, decodedData []byte, schemaData *MockSchema) {
	if serializedData != nil {
		m.MockSerializedData = serializedData
	}
	if decodedData != nil {
		m.MockDecodedData = decodedData
	}
	if schemaData != nil {
		m.MockSchemaData = schemaData
	}
}

// GetCallCounts returns the number of times each operation was called
func (m *SimpleMockSwigOperations) GetCallCounts() map[string]int {
	return map[string]int{
		"NewSerializer":      m.NewSerializerCalls,
		"NewDeserializer":    m.NewDeserializerCalls,
		"NewSchema":          m.NewSchemaCalls,
		"SerializerEncode":   m.SerializerEncodeCalls,
		"DeserializerDecode": m.DeserializerDecodeCalls,
	}
}

// Reset clears all call counts and resets to default state
func (m *SimpleMockSwigOperations) Reset() {
	m.SerializerShouldFail = false
	m.DeserializerShouldFail = false
	m.SchemaShouldFail = false
	m.NewSerializerCalls = 0
	m.NewDeserializerCalls = 0
	m.NewSchemaCalls = 0
	m.SerializerEncodeCalls = 0
	m.DeserializerDecodeCalls = 0
}

// String provides a readable representation of the mock state
func (m *SimpleMockSwigOperations) String() string {
	return fmt.Sprintf("SimpleMockSwigOperations{SerializerCalls: %d, DeserializerCalls: %d, SchemaCalls: %d}",
		m.NewSerializerCalls, m.NewDeserializerCalls, m.NewSchemaCalls)
}
