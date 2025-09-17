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
type Deserializer struct {
	deserializer *C.glue_schema_registry_deserializer
	closed       bool
}

// NewDeserializer creates a new deserializer instance
func NewDeserializer(configPath string) (*Deserializer, error) {

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

	runtime.SetFinalizer(d, cleanupDeserializer)
	return d, nil
}

func cleanupDeserializer(d *Deserializer) {
	d.finalize()
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

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	roba, robaErr := createReadOnlyByteArray(data)
	defer cleanupReadOnlyByteArray(roba)
	if robaErr != nil {
		return nil, robaErr
	}
	

	// Decode the data
	mba := C.glue_schema_registry_deserializer_decode(d.deserializer, roba, errHolder)
	defer cleanupMutableByteArray(mba)
	if *errHolder != nil {
		return nil, extractError("decode", *errHolder)
	}

	if mba == nil {
		return nil, fmt.Errorf("decode: %w", ErrMemoryAllocation)
	}

	result := mutableByteArrayToGoSlice(mba)
	

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

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	roba, robaErr := createReadOnlyByteArray(data)
	defer cleanupReadOnlyByteArray(roba)
	if robaErr != nil {
		return false, robaErr
	}


	canDecode := C.glue_schema_registry_deserializer_can_decode(d.deserializer, roba, errHolder)

	if *errHolder != nil {
		return false, extractError("can decode", *errHolder)
	}

	return bool(canDecode), nil
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

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	roba, robaErr := createReadOnlyByteArray(data)
	defer cleanupReadOnlyByteArray(roba)
	if robaErr != nil {
		return nil, robaErr
	}
	

	glueSchema := C.glue_schema_registry_deserializer_decode_schema(d.deserializer, roba, errHolder)
	defer cleanupGlueSchema(glueSchema)
	if *errHolder != nil {
		return nil, extractError("decode schema", *errHolder)
	}

	if glueSchema == nil {
		return nil, fmt.Errorf("decode schema %v", ErrInvalidSchema)
	}

	// Convert to Schema and cleanup
	result := extractSchemaFromGlue(glueSchema)
	

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


	return nil
}

// finalize is called by the garbage collector as a safety net
func (d *Deserializer) finalize() {
	_ = d.Close()
}
