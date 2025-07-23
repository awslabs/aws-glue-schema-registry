package gsrserde

import (
	"errors"
	"fmt"
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
	Code    int
	Message string
}

func (e *SchemaRegistryError) Error() string {
	return fmt.Sprintf("schema registry error (code %d): %s", e.Code, e.Message)
}

// wrapError wraps a native error with context
func wrapError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}
