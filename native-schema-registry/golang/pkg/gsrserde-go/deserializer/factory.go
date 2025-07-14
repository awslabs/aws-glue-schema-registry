package deserializer

import (
	"fmt"
	"sync"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer/protobuf"
)

var (
	// ErrUnsupportedDataFormat is returned when an unsupported data format is requested
	ErrUnsupportedDataFormat = fmt.Errorf("unsupported data format")
)

// DeserializerFactory defines the interface for the deserializer factory
type DeserializerFactory interface {
	// GetDeserializer returns a format-specific deserializer instance
	GetDeserializer(dataFormat string, schema *gsrserde.Schema) (DataFormatDeserializer, error)
	
	// ClearCache clears all cached deserializers
	ClearCache()
	
	// GetCacheStats returns statistics about cached deserializers
	GetCacheStats() map[string]int
}

// DataFormatDeserializerFactory creates and manages format-specific deserializers.
// It uses a singleton pattern to ensure thread-safe access and efficient resource usage.
type DataFormatDeserializerFactory struct {
	// protobufDeserializerMap holds cached protobuf deserializers
	protobufDeserializers map[string]*protobuf.ProtobufDeserializer
	
	// avroDeserializerMap holds cached avro deserializers (to be implemented)
	// avroDeserializers map[string]*avro.AvroDeserializer
	
	// jsonDeserializerMap holds cached json deserializers (to be implemented)  
	// jsonDeserializers map[string]*json.JsonDeserializer
	
	// mu protects concurrent access to the maps
	mu sync.RWMutex
}

var (
	// factoryInstance holds the singleton instance
	factoryInstance *DataFormatDeserializerFactory
	
	// factoryOnce ensures the factory is initialized only once
	factoryOnce sync.Once
)

// GetInstance returns the singleton instance of the deserializer factory.
// This ensures thread-safe access and efficient resource usage.
func GetInstance() *DataFormatDeserializerFactory {
	factoryOnce.Do(func() {
		factoryInstance = &DataFormatDeserializerFactory{
			protobufDeserializers: make(map[string]*protobuf.ProtobufDeserializer),
			// avroDeserializers: make(map[string]*avro.AvroDeserializer),
			// jsonDeserializers: make(map[string]*json.JsonDeserializer),
		}
	})
	return factoryInstance
}

// GetDeserializerFactory returns the singleton deserializer factory as an interface.
// This is the recommended way to access the factory from external packages.
func GetDeserializerFactory() DeserializerFactory {
	return GetInstance()
}

// GetDeserializer returns a format-specific deserializer instance.
// It creates or retrieves cached deserializers based on the data format.
//
// Parameters:
//   dataFormat: The data format ("AVRO", "PROTOBUF", "JSON")
//   schema: The schema information (used for caching key)
//
// Returns:
//   DataFormatDeserializer: The format-specific deserializer
//   error: Any error that occurred during deserializer creation
func (f *DataFormatDeserializerFactory) GetDeserializer(dataFormat string, schema *gsrserde.Schema) (DataFormatDeserializer, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	switch dataFormat {
	case "PROTOBUF":
		return f.getProtobufDeserializer(schema)
	case "AVRO":
		// TODO: Implement Avro deserializer
		return nil, fmt.Errorf("%w: AVRO deserializer not yet implemented", ErrUnsupportedDataFormat)
	case "JSON":
		// TODO: Implement JSON deserializer
		return nil, fmt.Errorf("%w: JSON deserializer not yet implemented", ErrUnsupportedDataFormat)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDataFormat, dataFormat)
	}
}

// getProtobufDeserializer returns a cached or new protobuf deserializer instance.
// It uses the schema's additional info (message type) as the cache key.
func (f *DataFormatDeserializerFactory) getProtobufDeserializer(schema *gsrserde.Schema) (*protobuf.ProtobufDeserializer, error) {
	// Use schema's additional info as cache key (should contain message type name)
	// If not available, use a default key
	cacheKey := schema.AdditionalInfo
	if cacheKey == "" {
		cacheKey = "default"
	}
	
	// Check if we already have a cached deserializer
	f.mu.RLock()
	if deserializer, exists := f.protobufDeserializers[cacheKey]; exists {
		f.mu.RUnlock()
		return deserializer, nil
	}
	f.mu.RUnlock()
	
	// Create a new deserializer
	f.mu.Lock()
	defer f.mu.Unlock()
	
	// Double-check in case another goroutine created it while we were waiting
	if deserializer, exists := f.protobufDeserializers[cacheKey]; exists {
		return deserializer, nil
	}
	
	// Create and cache the new deserializer
	deserializer := protobuf.NewProtobufDeserializer()
	f.protobufDeserializers[cacheKey] = deserializer
	
	return deserializer, nil
}

// TODO: Implement getAvroDeserializer when AvroDeserializer is ready
// func (f *DataFormatDeserializerFactory) getAvroDeserializer(schema *gsrserde.Schema) (*avro.AvroDeserializer, error) {
//     // Implementation will be similar to getProtobufDeserializer
// }

// TODO: Implement getJsonDeserializer when JsonDeserializer is ready  
// func (f *DataFormatDeserializerFactory) getJsonDeserializer(schema *gsrserde.Schema) (*json.JsonDeserializer, error) {
//     // Implementation will be similar to getProtobufDeserializer
// }

// ClearCache clears all cached deserializers.
// This can be useful for testing or when you want to force recreation of deserializers.
func (f *DataFormatDeserializerFactory) ClearCache() {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	f.protobufDeserializers = make(map[string]*protobuf.ProtobufDeserializer)
	// f.avroDeserializers = make(map[string]*avro.AvroDeserializer)
	// f.jsonDeserializers = make(map[string]*json.JsonDeserializer)
}

// GetCacheStats returns statistics about the cached deserializers.
// This can be useful for monitoring and debugging.
func (f *DataFormatDeserializerFactory) GetCacheStats() map[string]int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	return map[string]int{
		"protobuf": len(f.protobufDeserializers),
		// "avro":     len(f.avroDeserializers),
		// "json":     len(f.jsonDeserializers),
	}
}
