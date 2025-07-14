package gsrserde

import (
	"runtime"
	"sync"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/GsrSerDe"
)

// Deserializer is a thread-safe wrapper around the native schema registry deserializer
type Deserializer struct {
	deserializer GsrSerDe.Glue_schema_registry_deserializer
	closed       bool
	mu           sync.Mutex
}

// NewDeserializer creates a new deserializer instance
func NewDeserializer() (*Deserializer, error) {
	// Create error holder
	err := createErrorHolder()
	
	// Create native deserializer
	deserializer := GsrSerDe.NewGlue_schema_registry_deserializer(err)
	
	// Check for errors
	if err != nil && err.Swigcptr() != 0 {
		return nil, extractError("create deserializer", err)
	}
	
	if deserializer == nil || deserializer.Swigcptr() == 0 {
		return nil, ErrInitializationFailed
	}
	
	d := &Deserializer{
		deserializer: deserializer,
		closed:       false,
	}
	
	// Set finalizer as a safety net
	runtime.SetFinalizer(d, (*Deserializer).finalize)
	
	return d, nil
}

// Decode deserializes the encoded data
func (d *Deserializer) Decode(data []byte) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	if d.closed {
		return nil, ErrClosed
	}
	
	if data == nil {
		return nil, ErrNilData
	}
	
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	
	// Create error holder
	err := createErrorHolder()
	
	// Create read-only byte array
	roba, robaErr := createReadOnlyByteArray(data, err)
	if robaErr != nil {
		return nil, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)
	
	// Decode the data
	mba := d.deserializer.Decode(roba, err)
	
	// Check for errors
	if err != nil && err.Swigcptr() != 0 {
		return nil, extractError("decode", err)
	}
	
	if mba == nil || mba.Swigcptr() == 0 {
		return nil, wrapError("decode", ErrMemoryAllocation)
	}
	
	// Convert to Go slice and cleanup
	result := mutableByteArrayToGoSlice(mba)
	cleanupMutableByteArray(mba)
	
	return result, nil
}

// CanDecode checks if the data can be decoded
func (d *Deserializer) CanDecode(data []byte) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	if d.closed {
		return false, ErrClosed
	}
	
	if data == nil {
		return false, ErrNilData
	}
	
	if len(data) == 0 {
		return false, ErrEmptyData
	}
	
	// Create error holder
	err := createErrorHolder()
	
	// Create read-only byte array
	roba, robaErr := createReadOnlyByteArray(data, err)
	if robaErr != nil {
		return false, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)
	
	// Check if can decode
	canDecode := d.deserializer.Can_decode(roba, err)
	
	// Check for errors
	if err != nil && err.Swigcptr() != 0 {
		return false, extractError("can decode", err)
	}
	
	return canDecode, nil
}

// DecodeSchema extracts the schema from encoded data
func (d *Deserializer) DecodeSchema(data []byte) (*Schema, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	if d.closed {
		return nil, ErrClosed
	}
	
	if data == nil {
		return nil, ErrNilData
	}
	
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	
	// Create error holder
	err := createErrorHolder()
	
	// Create read-only byte array
	roba, robaErr := createReadOnlyByteArray(data, err)
	if robaErr != nil {
		return nil, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)
	
	// Decode schema
	glueSchema := d.deserializer.Decode_schema(roba, err)
	
	// Check for errors
	if err != nil && err.Swigcptr() != 0 {
		return nil, extractError("decode schema", err)
	}
	
	if glueSchema == nil || glueSchema.Swigcptr() == 0 {
		return nil, wrapError("decode schema", ErrInvalidSchema)
	}
	
	// Convert to Schema and cleanup
	result := extractSchemaFromGlue(glueSchema)
	cleanupGlueSchema(glueSchema)
	
	return result, nil
}

// Close releases all resources associated with the deserializer
func (d *Deserializer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	if d.closed {
		return nil
	}
	
	d.closed = true
	
	if d.deserializer != nil && d.deserializer.Swigcptr() != 0 {
		GsrSerDe.DeleteGlue_schema_registry_deserializer(d.deserializer)
		d.deserializer = nil
	}
	
	// Remove finalizer
	runtime.SetFinalizer(d, nil)
	
	return nil
}

// finalize is called by the garbage collector as a safety net
func (d *Deserializer) finalize() {
	_ = d.Close()
}
