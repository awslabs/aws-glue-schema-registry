package deserializer

import (
	"fmt"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// stringToDataFormat converts a string representation of data format to DataFormat enum
func stringToDataFormat(dataFormatStr string) (common.DataFormat, error) {
	switch dataFormatStr {
	case "AVRO":
		return common.DataFormatAvro, nil
	case "JSON":
		return common.DataFormatJSON, nil
	case "PROTOBUF":
		return common.DataFormatProtobuf, nil
	default:
		return common.DataFormatUnknown, fmt.Errorf("unsupported data format: %s", dataFormatStr)
	}
}

// KafkaDeserializerConfig holds configuration for the Kafka deserializer
type KafkaDeserializerConfig struct {
	// AWS Configuration
	Region       string
	RegistryName string

	// Caching Configuration
	CacheSize int
	CacheTTL  int // in seconds

	// Compression Configuration
	CompressionType string

	// Additional configuration options
	AdditionalConfig map[string]interface{}
}

// DefaultKafkaDeserializerConfig returns a default configuration
func DefaultKafkaDeserializerConfig() *KafkaDeserializerConfig {
	return &KafkaDeserializerConfig{
		Region:           "us-east-1",
		CacheSize:        100,
		CacheTTL:         3600, // 1 hour
		CompressionType:  "none",
		AdditionalConfig: make(map[string]interface{}),
	}
}

// KafkaDeserializer is a Kafka-specific deserializer that mirrors the C# implementation
// It provides a high-level interface for deserializing Kafka messages using AWS Glue Schema Registry
// NOTE: This deserializer is NOT thread-safe. Each instance should be used by
// only one context/operation to comply with native library constraints.
type KafkaDeserializer struct {
	coreDeserializer *gsrserde.Deserializer
	formatFactory    DeserializerFactory
	config           *KafkaDeserializerConfig
	closed           bool
}

// NewKafkaDeserializer creates a new Kafka deserializer instance
func NewKafkaDeserializer(config *KafkaDeserializerConfig) (*KafkaDeserializer, error) {
	if config == nil {
		config = DefaultKafkaDeserializerConfig()
	}

	// Create core deserializer for GSR operations
	coreDeserializer, err := gsrserde.NewDeserializer()
	if err != nil {
		return nil, fmt.Errorf("failed to create core deserializer: %w", err)
	}

	// Get format deserializer factory
	formatFactory := GetDeserializerFactory()

	return &KafkaDeserializer{
		coreDeserializer: coreDeserializer,
		formatFactory:    formatFactory,
		config:           config,
		closed:           false,
	}, nil
}

// NewKafkaDeserializerWithDefaults creates a new Kafka deserializer with default configuration
func NewKafkaDeserializerWithDefaults() (*KafkaDeserializer, error) {
	return NewKafkaDeserializer(DefaultKafkaDeserializerConfig())
}

// Configure updates the deserializer configuration
func (kd *KafkaDeserializer) Configure(config *KafkaDeserializerConfig) error {
	if kd.closed {
		return gsrserde.ErrClosed
	}

	if config == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	kd.config = config
	return nil
}

// Deserialize deserializes a Kafka message using AWS Glue Schema Registry
// This method mirrors the C# GlueSchemaRegistryKafkaDeserializer.Deserialize method
func (kd *KafkaDeserializer) Deserialize(topic string, data []byte) (interface{}, error) {
	if kd.closed {
		return nil, gsrserde.ErrClosed
	}

	// Handle nil data case (mirrors C# behavior)
	if data == nil {
		return nil, nil
	}

	// Check if data can be decoded (mirrors C# CanDecode check)
	canDecode, err := kd.coreDeserializer.CanDecode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to check if data can be decoded: %w", err)
	}

	if !canDecode {
		return nil, fmt.Errorf("byte data cannot be decoded: data does not contain GSR header")
	}

	// Decode the GSR-wrapped bytes (mirrors C# Decode call)
	decodedBytes, err := kd.coreDeserializer.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode GSR data: %w", err)
	}

	// Extract schema information (mirrors C# DecodeSchema call)
	schema, err := kd.coreDeserializer.DecodeSchema(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode schema: %w", err)
	}

	// Create configuration from schema information
	configMap := make(map[string]interface{})
	
	// Convert schema DataFormat string to DataFormat enum
	dataFormat, err := stringToDataFormat(schema.DataFormat)
	if err != nil {
		return nil, err
	}
	
	configMap[common.DataFormatTypeKey] = dataFormat
	
	// Create configuration object
	config := common.NewConfiguration(configMap)
	
	// Get the appropriate format deserializer (mirrors C# factory.GetDeserializer call)
	formatDeserializer, err := kd.formatFactory.GetDeserializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get deserializer for format %s: %w", schema.DataFormat, err)
	}

	// Deserialize the actual message content (mirrors C# deserializer.Deserialize call)
	result, err := formatDeserializer.Deserialize(decodedBytes, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize %s data: %w", dataFormat, err)
	}

	return result, nil
}

// CanDeserialize checks if the provided data can be deserialized
func (kd *KafkaDeserializer) CanDeserialize(data []byte) (bool, error) {
	if kd.closed {
		return false, gsrserde.ErrClosed
	}

	if data == nil {
		return false, nil
	}

	return kd.coreDeserializer.CanDecode(data)
}

// GetSchema extracts schema information from the data without deserializing the message
func (kd *KafkaDeserializer) GetSchema(data []byte) (*gsrserde.Schema, error) {
	if kd.closed {
		return nil, gsrserde.ErrClosed
	}

	if data == nil {
		return nil, gsrserde.ErrNilData
	}

	return kd.coreDeserializer.DecodeSchema(data)
}

// GetConfiguration returns the current configuration
func (kd *KafkaDeserializer) GetConfiguration() *KafkaDeserializerConfig {
	// Return a copy to prevent external modification
	configCopy := *kd.config
	configCopy.AdditionalConfig = make(map[string]interface{})
	for k, v := range kd.config.AdditionalConfig {
		configCopy.AdditionalConfig[k] = v
	}

	return &configCopy
}

// Close releases all resources associated with the deserializer
func (kd *KafkaDeserializer) Close() error {
	if kd.closed {
		return nil
	}

	kd.closed = true

	// Close core deserializer
	if kd.coreDeserializer != nil {
		err := kd.coreDeserializer.Close()
		if err != nil {
			return fmt.Errorf("failed to close core deserializer: %w", err)
		}
	}

	// No need to clear factory cache since we don't cache instances anymore

	return nil
}

// IsClosed returns whether the deserializer is closed
func (kd *KafkaDeserializer) IsClosed() bool {
	return kd.closed
}
