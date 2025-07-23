package serializer

import (
	"fmt"
	"sync"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/json"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/protobuf"
)

var (
	// ErrUnsupportedDataFormat is returned when an unsupported data format is requested
	ErrUnsupportedDataFormat = fmt.Errorf("unsupported data format")
)

// SerializerFactory defines the interface for the serializer factory
type SerializerFactory interface {
	// GetSerializer returns a format-specific serializer instance
	GetSerializer(config *common.Configuration) (DataFormatSerializer, error)
}

// DataFormatSerializerFactory creates format-specific serializers.
// NOTE: This factory does NOT cache instances. Each call to GetSerializer
// returns a fresh instance to comply with native library constraints that
// prohibit sharing serializers across contexts.
type DataFormatSerializerFactory struct {
	// No caching - each request gets a fresh instance
}

var (
	// factoryInstance holds the singleton factory instance
	factoryInstance *DataFormatSerializerFactory

	// factoryOnce ensures the factory is initialized only once
	factoryOnce sync.Once
)

// GetInstance returns the singleton instance of the serializer factory.
func GetInstance() *DataFormatSerializerFactory {
	factoryOnce.Do(func() {
		factoryInstance = &DataFormatSerializerFactory{}
	})
	return factoryInstance
}

// GetSerializerFactory returns the singleton serializer factory as an interface.
// This is the recommended way to access the factory from external packages.
func GetSerializerFactory() SerializerFactory {
	return GetInstance()
}

// GetSerializer returns a format-specific serializer instance.
// IMPORTANT: Each call creates a fresh instance to comply with native library
// constraints that prohibit sharing serializers across contexts.
//
// Parameters:
//
//	config: The configuration containing data format and other settings
//
// Returns:
//
//	DataFormatSerializer: A fresh format-specific serializer instance
//	error: Any error that occurred during serializer creation
func (f *DataFormatSerializerFactory) GetSerializer(config *common.Configuration) (DataFormatSerializer, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}

	dataFormat := config.DataFormat()
	switch dataFormat {
	case common.DataFormatProtobuf:
		return protobuf.NewProtobufSerializer(config), nil
	case common.DataFormatAvro:
		return avro.NewAvroSerializer(config), nil
	case common.DataFormatJSON:
		return json.NewJsonSerializer(config), nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedDataFormat, dataFormat)
	}
}
