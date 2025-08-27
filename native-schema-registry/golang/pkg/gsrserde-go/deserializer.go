package gsrserde

import (
	"fmt"
	"runtime"
	"unsafe"
)

/*
#cgo CFLAGS: -w
#cgo CFLAGS: -I../../lib/include
#cgo LDFLAGS: -Wl,-rpath,${SRCDIR}/../../lib
#cgo LDFLAGS: -L../../lib -lnativeschemaregistry -lnative_schema_registry_c -lnative_schema_registry_c_data_types -laws_common_memalloc
#include "../../lib/include/glue_schema_registry_deserializer.h"
#include "../../lib/include/glue_schema_registry_error.h"
*/
import "C"

// Deserializer is a wrapper around the native schema registry deserializer
// NOTE: This wrapper is NOT thread-safe. Each instance should be used by
// only one context/operation to comply with native library constraints.
type Deserializer struct {
	deserializer *C.glue_schema_registry_deserializer
	closed       bool
}

// NewDeserializer creates a new deserializer instance
// Locks the goroutine to the thread until .Close() is called.
// As cleanup of these resources must come from the same thread
// .Close() will free memory and the underlying C structs. And must be called at some point to prevent memory leaks.
func NewDeserializer(configPath string) (*Deserializer, error) {
	runtime.LockOSThread()

	cString := C.CString(configPath)
	defer C.free(unsafe.Pointer(cString))

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	// Create native deserializer
	deserializer := C.new_glue_schema_registry_deserializer(cString, errHolder)

	if *errHolder != nil {
		return nil, extractError("create deserializer", *errHolder)
	}

	if deserializer == nil {
		return nil, ErrInitializationFailed
	}

	d := &Deserializer{
		deserializer: deserializer,
		closed:       false,
	}

	return d, nil
}

func cleanupDeserializer(d *Deserializer) {
	d.finalize()
}

// Decode deserializes the encoded data
func (d *Deserializer) Decode(data []byte) ([]byte, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if d.closed {
		return nil, ErrClosed
	}

	if data == nil {
		return nil, ErrNilData
	}

	if len(data) == 0 {
		return nil, ErrEmptyData
	}

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	roba, robaErr := createReadOnlyByteArray(data)
	if robaErr != nil {
		return nil, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)

	// Decode the data
	mba := C.glue_schema_registry_deserializer_decode(d.deserializer, roba, errHolder)

	if *errHolder != nil {
		return nil, extractError("decode", *errHolder)
	}

	if mba == nil {
		return nil, fmt.Errorf("decode: %w", ErrMemoryAllocation)
	}

	result := mutableByteArrayToGoSlice(mba)
	cleanupMutableByteArray(mba)

	return result, nil
}

// CanDecode checks if the data can be decoded
func (d *Deserializer) CanDecode(data []byte) (bool, error) {

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if d.closed {
		return false, ErrClosed
	}

	if data == nil {
		return false, ErrNilData
	}

	if len(data) == 0 {
		return false, ErrEmptyData
	}

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	roba, robaErr := createReadOnlyByteArray(data)
	if robaErr != nil {
		return false, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)

	canDecode := C.glue_schema_registry_deserializer_can_decode(d.deserializer, roba, errHolder)

	if *errHolder != nil {
		return false, extractError("can decode", *errHolder)
	}

	return bool(canDecode), nil
}

// DecodeSchema extracts the schema from encoded data
func (d *Deserializer) DecodeSchema(data []byte) (*Schema, error) {

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if d.closed {
		return nil, ErrClosed
	}

	if data == nil {
		return nil, ErrNilData
	}

	if len(data) == 0 {
		return nil, ErrEmptyData
	}

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	roba, robaErr := createReadOnlyByteArray(data)
	if robaErr != nil {
		return nil, robaErr
	}
	defer cleanupReadOnlyByteArray(roba)

	glueSchema := C.glue_schema_registry_deserializer_decode_schema(d.deserializer, roba, errHolder)

	if *errHolder != nil {
		return nil, extractError("decode schema", *errHolder)
	}

	if glueSchema == nil {
		return nil, fmt.Errorf("decode schema", ErrInvalidSchema)
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

	if d.deserializer != nil {
		C.delete_glue_schema_registry_deserializer(d.deserializer)
		d.deserializer = nil
	}

	runtime.UnlockOSThread()

	return nil
}

// finalize is called by the garbage collector as a safety net
func (d *Deserializer) finalize() {
	_ = d.Close()
}
