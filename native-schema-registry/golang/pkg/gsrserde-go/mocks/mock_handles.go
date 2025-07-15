package mocks

// MockHandle provides a simple mock handle that can be used in place of SWIG types during testing
// This avoids the need for C compilation when running tests
type MockHandle struct {
	ID   string
	Data interface{}
}

// MockSerializer simulates a SWIG Glue_schema_registry_serializer
type MockSerializer struct {
	*MockHandle
}

// NewMockSerializer creates a new mock serializer handle
func NewMockSerializer(id string) *MockSerializer {
	return &MockSerializer{
		MockHandle: &MockHandle{ID: id},
	}
}

// MockDeserializer simulates a SWIG Glue_schema_registry_deserializer
type MockDeserializer struct {
	*MockHandle
}

// NewMockDeserializer creates a new mock deserializer handle
func NewMockDeserializer(id string) *MockDeserializer {
	return &MockDeserializer{
		MockHandle: &MockHandle{ID: id},
	}
}

// MockSchema simulates a SWIG Glue_schema_registry_schema
type MockSchema struct {
	*MockHandle
	Name           string
	Definition     string
	DataFormat     string
	AdditionalInfo string
}

// NewMockSchema creates a new mock schema handle
func NewMockSchema(name, definition, dataFormat, additionalInfo string) *MockSchema {
	return &MockSchema{
		MockHandle:     &MockHandle{ID: "schema-" + name},
		Name:           name,
		Definition:     definition,
		DataFormat:     dataFormat,
		AdditionalInfo: additionalInfo,
	}
}

// MockReadOnlyByteArray simulates a SWIG Read_only_byte_array
type MockReadOnlyByteArray struct {
	*MockHandle
	Data []byte
}

// NewMockReadOnlyByteArray creates a new mock read-only byte array
func NewMockReadOnlyByteArray(data []byte) *MockReadOnlyByteArray {
	return &MockReadOnlyByteArray{
		MockHandle: &MockHandle{ID: "roba"},
		Data:       data,
	}
}

// MockMutableByteArray simulates a SWIG Mutable_byte_array
type MockMutableByteArray struct {
	*MockHandle
	Data   []byte
	MaxLen int64
}

// NewMockMutableByteArray creates a new mock mutable byte array
func NewMockMutableByteArray(data []byte) *MockMutableByteArray {
	return &MockMutableByteArray{
		MockHandle: &MockHandle{ID: "mba"},
		Data:       data,
		MaxLen:     int64(len(data)),
	}
}

// Get_data simulates getting data from mutable byte array
func (m *MockMutableByteArray) Get_data() *byte {
	if len(m.Data) == 0 {
		return nil
	}
	return &m.Data[0]
}

// Get_max_len simulates getting max length from mutable byte array
func (m *MockMutableByteArray) Get_max_len() int64 {
	return m.MaxLen
}

// MockError simulates a SWIG Glue_schema_registry_error
type MockError struct {
	*MockHandle
	ErrorMessage string
	ErrorCode    int
}

// NewMockError creates a new mock error
func NewMockError(message string, code int) *MockError {
	return &MockError{
		MockHandle:   &MockHandle{ID: "error"},
		ErrorMessage: message,
		ErrorCode:    code,
	}
}

// Swigcptr simulates the SWIG pointer check
func (m *MockHandle) Swigcptr() uintptr {
	if m == nil {
		return 0
	}
	// Return a non-zero value to simulate a valid pointer
	return 1
}
