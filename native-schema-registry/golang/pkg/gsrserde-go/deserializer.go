package gsrserde

import (
	"runtime"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/GsrSerDe"
)

// Deserializer is a wrapper around the native schema registry deserializer
// NOTE: This wrapper is NOT thread-safe. Each instance should be used by
// only one context/operation to comply with native library constraints.
type Deserializer struct {
	deserializer GsrSerDe.Glue_schema_registry_deserializer
	closed       bool
}

// NewDeserializer creates a new deserializer instance
// Locks the goroutine to the thread until .Close() is called. 
// As cleanup of these resources must come from the same thread
// .Close() will free memory and the underlying C structs. And must be called at some point to prevent memory leaks.
func NewDeserializer(configPath string) (*Deserializer, error) {
	runtime.LockOSThread()
	err := createErrorHolder()
	
	// Create native deserializer
	deserializer := GsrSerDe.NewGlue_schema_registry_deserializer(configPath,err)
	
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
	
	return d, nil
}

// Decode deserializes the encoded data
func (d *Deserializer) Decode(data []byte) ([]byte, error) {
	if d.closed {
		return nil, ErrClosed
	}
	
	if data == nil {
		return nil, ErrNilData
	}
	
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	
	err := createErrorHolder()
	
	roba, robaErr := createReadOnlyByteArray(data, err)
	if robaErr != nil {
		return nil, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)
	
	// Decode the data
	mba := d.deserializer.Decode(roba, err)
	
	if err != nil && err.Swigcptr() != 0 {
		return nil, extractError("decode", err)
	}
	
	if mba == nil || mba.Swigcptr() == 0 {
		return nil, wrapError("decode", ErrMemoryAllocation)
	}
	
	result := mutableByteArrayToGoSlice(mba)
	cleanupMutableByteArray(mba)
	
	return result, nil
}

// CanDecode checks if the data can be decoded
func (d *Deserializer) CanDecode(data []byte) (bool, error) {
	if d.closed {
		return false, ErrClosed
	}
	
	if data == nil {
		return false, ErrNilData
	}
	
	if len(data) == 0 {
		return false, ErrEmptyData
	}
	
	err := createErrorHolder()
	
	roba, robaErr := createReadOnlyByteArray(data, err)
	if robaErr != nil {
		return false, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)
	
	canDecode := d.deserializer.Can_decode(roba, err)
	
	if err != nil && err.Swigcptr() != 0 {
		return false, extractError("can decode", err)
	}
	
	return canDecode, nil
}

// DecodeSchema extracts the schema from encoded data
func (d *Deserializer) DecodeSchema(data []byte) (*Schema, error) {
	if d.closed {
		return nil, ErrClosed
	}
	
	if data == nil {
		return nil, ErrNilData
	}
	
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	
	err := createErrorHolder()
	
	roba, robaErr := createReadOnlyByteArray(data, err)
	if robaErr != nil {
		return nil, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)
	
	glueSchema := d.deserializer.Decode_schema(roba, err)
	
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
	if d == nil {
		return nil
	}
	if d.closed {
		return nil
	}
	
	d.closed = true
	
	if d.deserializer != nil && d.deserializer.Swigcptr() != 0 {
		GsrSerDe.DeleteGlue_schema_registry_deserializer(d.deserializer)
		d.deserializer = nil
	}
	
	runtime.UnlockOSThread()
	
	return nil
}

// finalize is called by the garbage collector as a safety net
func (d *Deserializer) finalize() {
	_ = d.Close()
}
