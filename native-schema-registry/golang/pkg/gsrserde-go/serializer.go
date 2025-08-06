package gsrserde

import (
	"runtime"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/GsrSerDe"
)

// Serializer is a wrapper around the native schema registry serializer
// NOTE: This wrapper is NOT thread-safe. Each instance should be used by
// only one context/operation to comply with native library constraints.
type Serializer struct {
	serializer GsrSerDe.Glue_schema_registry_serializer
	closed     bool
}

// NewSerializer creates a new serializer instance
func NewSerializer() (*Serializer, error) {
	runtime.LockOSThread()
	err := createErrorHolder()
	
	// Create native serializer
	serializer := GsrSerDe.NewGlue_schema_registry_serializer(err)
	
	if err != nil && err.Swigcptr() != 0 {
		return nil, extractError("create serializer", err)
	}
	
	if serializer == nil || serializer.Swigcptr() == 0 {
		return nil, ErrInitializationFailed
	}
	
	s := &Serializer{
		serializer: serializer,
		closed:     false,
	}
	
	
	return s, nil
}

// Encode serializes data with the given schema
func (s *Serializer) Encode(data []byte, transportName string, schema *Schema) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}
	
	if data == nil {
		return nil, ErrNilData
	}
	
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	
	if schema == nil {
		return nil, ErrNilSchema
	}
	
	err := createErrorHolder()
	
	roba, robaErr := createReadOnlyByteArray(data, err)
	if robaErr != nil {
		return nil, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)
	
	glueSchema, schemaErr := createGlueSchema(schema, err)
	if schemaErr != nil {
		return nil, schemaErr
	}
	defer cleanupGlueSchema(glueSchema)
	
	mba := s.serializer.Encode(roba, transportName, glueSchema, err)
	
	if err != nil && err.Swigcptr() != 0 {
		return nil, extractError("encode", err)
	}
	
	if mba == nil || mba.Swigcptr() == 0 {
		return nil, wrapError("encode", ErrMemoryAllocation)
	}
	
	// Convert to Go slice and cleanup
	result := mutableByteArrayToGoSlice(mba)
	cleanupMutableByteArray(mba)
	
	return result, nil
}

// Close releases all resources associated with the serializer
func (s *Serializer) Close() error {
	if s.closed {
		return nil
	}
	
	s.closed = true
	
	if s.serializer != nil && s.serializer.Swigcptr() != 0 {
		GsrSerDe.DeleteGlue_schema_registry_serializer(s.serializer)
		s.serializer = nil
	}
	
	runtime.UnlockOSThread()
	
	return nil
}

// finalize is called by the garbage collector as a safety net
func (s *Serializer) finalize() {
	_ = s.Close()
}
