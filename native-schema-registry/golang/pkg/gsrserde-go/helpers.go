package gsrserde

import (
	"unsafe"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/GsrSerDe"
)

// createReadOnlyByteArray creates a SWIG Read_only_byte_array from Go byte slice
func createReadOnlyByteArray(data []byte, err GsrSerDe.Glue_schema_registry_error) (GsrSerDe.Read_only_byte_array, error) {
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	
	// Get pointer to first byte
	dataPtr := (*byte)(unsafe.Pointer(&data[0]))
	
	roba := GsrSerDe.NewRead_only_byte_array(dataPtr, int64(len(data)), err)
	
	if err != nil && err.Swigcptr() != 0 {
		return nil, extractError("create read only byte array", err)
	}
	
	return roba, nil
}

// mutableByteArrayToGoSlice converts a SWIG Mutable_byte_array to Go byte slice
func mutableByteArrayToGoSlice(mba GsrSerDe.Mutable_byte_array) []byte {
	if mba == nil || mba.Swigcptr() == 0 {
		return nil
	}
	
	// Get the data pointer and length
	dataPtr := mba.Get_data()
	maxLen := mba.Get_max_len()
	
	if dataPtr == nil || maxLen <= 0 {
		return nil
	}
	
	// Create a Go slice from the C memory
	// We need to copy the data to ensure memory safety
	result := make([]byte, maxLen)
	
	// Use unsafe to create a temporary slice view of the C memory
	cSlice :=  unsafe.Slice((*byte)(dataPtr), maxLen)
	copy(result, cSlice)
	
	return result
}

// cleanupMutableByteArray safely deletes a mutable byte array
func cleanupMutableByteArray(mba GsrSerDe.Mutable_byte_array) {
	if mba != nil && mba.Swigcptr() != 0 {
		GsrSerDe.DeleteMutable_byte_array(mba)
	}
}

// cleanupReadOnlyByteArray safely deletes a read only byte array
func cleanupReadOnlyByteArray(roba GsrSerDe.Read_only_byte_array) {
	if roba != nil && roba.Swigcptr() != 0 {
		GsrSerDe.DeleteRead_only_byte_array(roba)
	}
}

// cleanupGlueSchema safely deletes a glue schema
func cleanupGlueSchema(schema GsrSerDe.Glue_schema_registry_schema) {
	if schema != nil && schema.Swigcptr() != 0 {
		GsrSerDe.DeleteGlue_schema_registry_schema(schema)
	}
}

// createErrorHolder creates a new error holder for C functions
// Note: In the SWIG bindings, the error is passed as a parameter, not returned
// We'll use a nil error for normal operations
func createErrorHolder() GsrSerDe.Glue_schema_registry_error {
	// In the SWIG interface, we just pass nil for the error parameter
	// The C code will allocate error if needed
	return nil
}
