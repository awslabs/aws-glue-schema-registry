package gsrserde

import (
	"errors"
	"fmt"
	"log"
	"unsafe"
	
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/GsrSerDe"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

type ErrorCode int

const (
	ErrInvalidState ErrorCode = 5000
	ErrNullParameters ErrorCode = 5001
	ErrGraalVMInitException ErrorCode = 5002
	ErrGraalVMTeardownException ErrorCode = 5003
	ErrInvalidParameters ErrorCode = 5004
	ErrRuntimeError ErrorCode = 5005
)

// cGlueSchemaRegistryError represents the C struct layout for glue_schema_registry_error
// This matches the struct defined in glue_schema_registry_error.h:
// typedef struct glue_schema_registry_error {
//     char * msg;
//     int code;
// } glue_schema_registry_error;
type cGlueSchemaRegistryError struct {
	msg  *C.char
	code C.int
}

var (
	// ErrClosed is returned when operations are attempted on a closed resource
	ErrClosed = errors.New("resource has been closed")
	
	// ErrNilData is returned when nil data is provided
	ErrNilData = errors.New("data cannot be nil")
	
	// ErrEmptyData is returned when empty data is provided
	ErrEmptyData = errors.New("data cannot be empty")
	
	// ErrNilSchema is returned when nil schema is provided
	ErrNilSchema = errors.New("schema cannot be nil")
	
	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = errors.New("invalid schema")
	
	// ErrInitializationFailed is returned when C library initialization fails
	ErrInitializationFailed = errors.New("failed to initialize native library")
	
	// ErrMemoryAllocation is returned when memory allocation fails
	ErrMemoryAllocation = errors.New("memory allocation failed")
)

// SchemaRegistryError represents an error from the native schema registry
type SchemaRegistryError struct {
	Message string
	Code   ErrorCode 
}

func (e *SchemaRegistryError) Error() string {
	return fmt.Sprintf("schema registry error (code %d): %s", e.Code, e.Message)
}

// extractError extracts error information from SWIG error interface with memory safety
func extractError(operation string, err GsrSerDe.Glue_schema_registry_error) error {
	if err == nil || err.Swigcptr() == 0 {
		return nil
	}
	
	// Add panic recovery for unsafe operations
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in extractError for operation '%s': %v", operation, r)
		}
	}()
	
	// Validate SWIG pointer is non-zero and seems reasonable
	swigPtr := err.Swigcptr()
	if swigPtr == 0 {
		return &SchemaRegistryError{
			Code:    ErrRuntimeError,
			Message: "null error pointer",
		}
	}
	
	// Cast SWIG pointer to C struct - this is the unsafe part
	cErr := (*cGlueSchemaRegistryError)(unsafe.Pointer(swigPtr))
	if cErr == nil {
		return &SchemaRegistryError{
			Code:    ErrRuntimeError,
			Message: "failed to cast error pointer",
		}
	}
	
	// Extract error code with validation
	rawCode := int(cErr.code)
	var code ErrorCode
	
	// Validate error code is in expected range
	switch rawCode {
	case 5000:
		code = ErrInvalidState
	case 5001:
		code = ErrNullParameters
	case 5002:
		code = ErrGraalVMInitException
	case 5003:
		code = ErrGraalVMTeardownException
	case 5004:
		code = ErrInvalidParameters
	case 5005:
		code = ErrRuntimeError
	default:
		// Unknown error code, use runtime error as fallback
		code = ErrRuntimeError
		log.Printf("Unknown error code %d in extractError, using ErrRuntimeError", rawCode)
	}
	
	// Extract message with safety checks
	var message string
	if cErr.msg != nil {
		// Copy C string to Go memory immediately to avoid dangling pointer
		message = C.GoString(cErr.msg)
		if message == "" {
			message = fmt.Sprintf("native schema registry error (code %d)", rawCode)
		}
	} else {
		message = fmt.Sprintf("native schema registry error (code %d) - no message", rawCode)
	}
	
	return &SchemaRegistryError{
		Code:    code,
		Message: message,
	}
}

// wrapError wraps a native error with context
func wrapError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}
