package serializer

import (
	"fmt"
	"sync"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/protobuf"
)

var (
	// ErrUnsupportedDataFormat is returned when an unsupported data format is requested
	ErrUnsupportedDataFormat = fmt.Errorf("unsupported data format")
)

// SerializerFactory defines the interface for the serializer factory
type SerializerFactory interface {
	// GetSerializer returns a format-specific serializer instance
	GetSerializer(dataFormat string, schema *gsrserde.Schema) (DataFormatSerializer, error)

	// ClearCache clears all cached serializers
	ClearCache()

	// GetCacheStats returns statistics about cached serializers
	GetCacheStats() map[string]int
}

// DataFormatSerializerFactory creates and manages format-specific serializers.
// It uses a singleton pattern to ensure thread-safe access and efficient resource usage.
type DataFormatSerializerFactory struct {
	// protobufSerializerMap holds cached protobuf serializers
	protobufSerializers map[string]*protobuf.ProtobufSerializer

	// avroSerializerMap holds cached avro serializers (to be implemented)
	// avroSerializers map[string]*avro.AvroSerializer

	// jsonSerializerMap holds cached json serializers (to be implemented)
	// jsonSerializers map[string]*json.JsonSerializer

	// mu protects concurrent access to the maps
	mu sync.RWMutex
}

var (
	// factoryInstance holds the singleton instance
	factoryInstance *DataFormatSerializerFactory

	// factoryOnce ensures the factory is initialized only once
	factoryOnce sync.Once
)

// GetInstance returns the singleton instance of the serializer factory.
// This ensures thread-safe access and efficient resource usage.
func GetInstance() *DataFormatSerializerFactory {
	factoryOnce.Do(func() {
		factoryInstance = &DataFormatSerializerFactory{
			protobufSerializers: make(map[string]*protobuf.ProtobufSerializer),
			// avroSerializers: make(map[string]*avro.AvroSerializer),
			// jsonSerializers: make(map[string]*json.JsonSerializer),
		}
	})
	return factoryInstance
}

// GetSerializerFactory returns the singleton serializer factory as an interface.
// This is the recommended way to access the factory from external packages.
func GetSerializerFactory() SerializerFactory {
	return GetInstance()
}

// GetSerializer returns a format-specific serializer instance.
// It creates or retrieves cached serializers based on the data format.
//
// Parameters:
//
//	dataFormat: The data format ("AVRO", "PROTOBUF", "JSON")
//	schema: The schema information (used for caching key)
//
// Returns:
//
//	DataFormatSerializer: The format-specific serializer
//	error: Any error that occurred during serializer creation
func (f *DataFormatSerializerFactory) GetSerializer(dataFormat string, schema *gsrserde.Schema) (DataFormatSerializer, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	switch dataFormat {
	case "PROTOBUF":
		return f.getProtobufSerializer(schema)
	case "AVRO":
		// TODO: Implement Avro serializer
		return nil, fmt.Errorf("%w: AVRO serializer not yet implemented", ErrUnsupportedDataFormat)
	case "JSON":
		// TODO: Implement JSON serializer
		return nil, fmt.Errorf("%w: JSON serializer not yet implemented", ErrUnsupportedDataFormat)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDataFormat, dataFormat)
	}
}

// getProtobufSerializer returns a cached or new protobuf serializer instance.
// It uses the schema's additional info (message type) as the cache key.
func (f *DataFormatSerializerFactory) getProtobufSerializer(schema *gsrserde.Schema) (*protobuf.ProtobufSerializer, error) {
	// Use schema's additional info as cache key (should contain message type name)
	// If not available, use a default key
	cacheKey := schema.AdditionalInfo
	if cacheKey == "" {
		cacheKey = "default"
	}

	// Check if we already have a cached serializer
	f.mu.RLock()
	if serializer, exists := f.protobufSerializers[cacheKey]; exists {
		f.mu.RUnlock()
		return serializer, nil
	}
	f.mu.RUnlock()

	// Create a new serializer
	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check in case another goroutine created it while we were waiting
	if serializer, exists := f.protobufSerializers[cacheKey]; exists {
		return serializer, nil
	}

	// Create and cache the new serializer
	serializer := protobuf.NewProtobufSerializer()
	f.protobufSerializers[cacheKey] = serializer

	return serializer, nil
}

// TODO: Implement getAvroSerializer when AvroSerializer is ready
// func (f *DataFormatSerializerFactory) getAvroSerializer(schema *gsrserde.Schema) (*avro.AvroSerializer, error) {
//     // Implementation will be similar to getProtobufSerializer
// }

// TODO: Implement getJsonSerializer when JsonSerializer is ready
// func (f *DataFormatSerializerFactory) getJsonSerializer(schema *gsrserde.Schema) (*json.JsonSerializer, error) {
//     // Implementation will be similar to getProtobufSerializer
// }

// ClearCache clears all cached serializers.
// This can be useful for testing or when you want to force recreation of serializers.
func (f *DataFormatSerializerFactory) ClearCache() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.protobufSerializers = make(map[string]*protobuf.ProtobufSerializer)
	// f.avroSerializers = make(map[string]*avro.AvroSerializer)
	// f.jsonSerializers = make(map[string]*json.JsonSerializer)
}

// GetCacheStats returns statistics about the cached serializers.
// This can be useful for monitoring and debugging.
func (f *DataFormatSerializerFactory) GetCacheStats() map[string]int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return map[string]int{
		"protobuf": len(f.protobufSerializers),
		// "avro":     len(f.avroSerializers),
		// "json":     len(f.jsonSerializers),
	}
}
