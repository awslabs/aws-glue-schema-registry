package gsrserde

import (
	"errors"
	"fmt"
	"log"
)

/*
#cgo CFLAGS: -w
#cgo CFLAGS: -I../../lib/include
#cgo LDFLAGS: -Wl,-rpath,${SRCDIR}/../../lib
#cgo LDFLAGS: -L../../lib -lnativeschemaregistry -lnative_schema_registry_c -lnative_schema_registry_c_data_types -laws_common_memalloc
#include "../../lib/include/glue_schema_registry_error.h"
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

// extractError extracts error information from C error pointer with memory safety
func extractError(operation string, err *C.glue_schema_registry_error) error {
	if err == nil {
		return nil
	}
	
	// Add panic recovery for unsafe operations
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in extractError for operation '%s': %v", operation, r)
		}
	}()
	
	// Extract error code with validation
	rawCode := int(err.code)
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
	if err.msg != nil {
		// Copy C string to Go memory immediately to avoid dangling pointer
		message = C.GoString(err.msg)
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
