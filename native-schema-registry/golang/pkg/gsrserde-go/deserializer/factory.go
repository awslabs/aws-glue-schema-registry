package deserializer

import (
	"fmt"
	"sync"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer/json"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer/protobuf"
)

var (
	// ErrUnsupportedDataFormat is returned when an unsupported data format is requested
	ErrUnsupportedDataFormat = fmt.Errorf("unsupported data format")
)

// DeserializerFactory defines the interface for the deserializer factory
type DeserializerFactory interface {
	// GetDeserializer returns a format-specific deserializer instance
	GetDeserializer(config *common.Configuration) (DataFormatDeserializer, error)
}

// DataFormatDeserializerFactory creates format-specific deserializers.
// NOTE: This factory does NOT cache instances. Each call to GetDeserializer 
// returns a fresh instance to comply with native library constraints that
// prohibit sharing deserializers across contexts.
type DataFormatDeserializerFactory struct {
	// No caching - each request gets a fresh instance
}

var (
	// factoryInstance holds the singleton factory instance
	factoryInstance *DataFormatDeserializerFactory
	
	// factoryOnce ensures the factory is initialized only once
	factoryOnce sync.Once
)

// GetInstance returns the singleton instance of the deserializer factory.
func GetInstance() *DataFormatDeserializerFactory {
	factoryOnce.Do(func() {
		factoryInstance = &DataFormatDeserializerFactory{}
	})
	return factoryInstance
}

// GetDeserializerFactory returns the singleton deserializer factory as an interface.
// This is the recommended way to access the factory from external packages.
func GetDeserializerFactory() DeserializerFactory {
	return GetInstance()
}

// GetDeserializer returns a format-specific deserializer instance.
// IMPORTANT: Each call creates a fresh instance to comply with native library
// constraints that prohibit sharing deserializers across contexts.
//
// Parameters:
//   config: The configuration containing data format and other settings
//
// Returns:
//   DataFormatDeserializer: A fresh format-specific deserializer instance
//   error: Any error that occurred during deserializer creation
func (f *DataFormatDeserializerFactory) GetDeserializer(config *common.Configuration) (DataFormatDeserializer, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}

	dataFormat := config.DataFormat()
	switch dataFormat {
	case common.DataFormatProtobuf:
		return protobuf.NewProtobufDeserializer(config), nil
	case common.DataFormatAvro:
		return avro.NewAvroDeserializer(config), nil
	case common.DataFormatJSON:
		return json.NewJsonDeserializer(config), nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedDataFormat, dataFormat)
	}
}
