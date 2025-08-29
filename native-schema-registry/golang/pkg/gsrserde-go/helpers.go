package gsrserde

import (
	"unsafe"
)

/*
#cgo CFLAGS: -w
#cgo CFLAGS: -I../../lib/include
#cgo LDFLAGS: -Wl,-rpath,${SRCDIR}/../../lib
#cgo LDFLAGS: -L../../lib -lnativeschemaregistry -lnative_schema_registry_c -lnative_schema_registry_c_data_types -laws_common_memalloc
#include "../../lib/include/read_only_byte_array.h"
#include "../../lib/include/mutable_byte_array.h"
#include "../../lib/include/glue_schema_registry_schema.h"
#include "../../lib/include/glue_schema_registry_error.h"
#include <stdlib.h>
*/
import "C"

// createReadOnlyByteArray creates a C read_only_byte_array from Go byte slice
func createReadOnlyByteArray(data []byte) (*C.read_only_byte_array, error) {

	errHolder := C.new_glue_schema_registry_error_holder()
	defer C.delete_glue_schema_registry_error_holder(errHolder)
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	
	// Get pointer to first byte
	dataPtr := (*C.uchar)(unsafe.Pointer(&data[0]))
	
	roba := C.new_read_only_byte_array(dataPtr, C.size_t(len(data)), errHolder)
	
	if *errHolder != nil {
		return nil, extractError("create read only byte array", *errHolder)
	}
	
	return roba, nil
}

// mutableByteArrayToGoSlice converts a C mutable_byte_array to Go byte slice
func mutableByteArrayToGoSlice(mba *C.mutable_byte_array) []byte {
	if mba == nil {
		return nil
	}
	
	// Get the data pointer and length
	dataPtr := C.mutable_byte_array_get_data(mba)
	maxLen := C.mutable_byte_array_get_max_len(mba)
	
	if dataPtr == nil || maxLen <= 0 {
		return nil
	}
	
	// Create a Go slice from the C memory
	// We need to copy the data to ensure memory safety
	result := make([]byte, maxLen)
	
	// Use unsafe to create a temporary slice view of the C memory
	cSlice := unsafe.Slice((*byte)(dataPtr), maxLen)
	copy(result, cSlice)
	
	return result
}

// cleanupMutableByteArray safely deletes a mutable byte array
func cleanupMutableByteArray(mba *C.mutable_byte_array) {
	if mba != nil {
		C.delete_mutable_byte_array(mba)
	}
}

// cleanupReadOnlyByteArray safely deletes a read only byte array
func cleanupReadOnlyByteArray(roba *C.read_only_byte_array) {
	if roba != nil {
		C.delete_read_only_byte_array(roba)
	}
}

// cleanupGlueSchema safely deletes a glue schema
func cleanupGlueSchema(schema *C.glue_schema_registry_schema) {
	if schema != nil {
		C.delete_glue_schema_registry_schema(schema)
	}
}
