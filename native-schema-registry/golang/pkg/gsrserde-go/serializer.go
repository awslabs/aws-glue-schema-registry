package gsrserde

import (
	"runtime"
	"unsafe"
)

/*
#include "../../lib/include/glue_schema_registry_serializer.h"
#include "../../lib/include/glue_schema_registry_error.h"
#include "../../lib/include/mutable_byte_array.h"
#include "../../lib/include/read_only_byte_array.h"
#include "../../lib/include/glue_schema_registry_schema.h"
#include <stdlib.h>
*/
import "C"

// Serializer is a wrapper around the native schema registry serializer
type Serializer struct {
	serializer *C.glue_schema_registry_serializer
	closed     bool
}

// NewSerializer creates a new serializer instance
func NewSerializer(configPath string) (*Serializer, error) {
	
	cString := C.CString(configPath)
	defer C.free(unsafe.Pointer(cString))
	
	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)
	
	serializer := C.new_glue_schema_registry_serializer(cString, errHolder)
	
	// Check for errors
	if *errHolder != nil {
		return nil,extractError("create serializer", *errHolder) 
	}
	
	if serializer == nil {
		return nil, ErrInitializationFailed
	}
	
	s := &Serializer{
		serializer: serializer,
		closed:     false,
	}
	
	
	runtime.SetFinalizer(s, cleanupSerializer)
	return s, nil
}

func cleanupSerializer(s *Serializer) {
	s.finalize()
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
	
	
	// Create read-only byte array from Go byte slice
	roba, robaErr := createReadOnlyByteArray(data)
	defer cleanupReadOnlyByteArray(roba)
	if robaErr != nil {
		return nil, robaErr
	}

	
	// Create glue schema from Schema struct
	glueSchema, schemaErr := createGlueSchema(schema)
	defer cleanupGlueSchema(glueSchema)
	if schemaErr != nil {
		return nil, schemaErr
	}
	
	
	// Convert transport name to C string
	cTransportName := C.CString(transportName)
	defer C.free(unsafe.Pointer(cTransportName))
	
	
	// Create error holder for this operation
	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)

	// Call C encode function
	mba := C.glue_schema_registry_serializer_encode(s.serializer, roba, cTransportName, glueSchema, errHolder)
	
	// Check for errors
	if *errHolder != nil {
		return nil, extractError("encode", *errHolder)
	}
	
	// Convert to Go slice and cleanup
	result := mutableByteArrayToGoSlice(mba)
	cleanupMutableByteArray(mba)
	
	return result, nil
}

// Close releases all resources associated with the serializer
func (s *Serializer) Close() error {
	if s == nil {
		return nil
	}
	if s.closed {
		return nil
	}
	
	s.closed = true
	
	if s.serializer != nil {
		C.delete_glue_schema_registry_serializer(s.serializer)
		s.serializer = nil
	}
	
	
	return nil
}

// finalize is called by the garbage collector as a safety net
func (s *Serializer) finalize() {
	_ = s.Close()
}
