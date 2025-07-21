package serializer

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

// KafkaSerializerConfig holds configuration for the Kafka serializer
type KafkaSerializerConfig struct {
	// AWS Configuration
	Region       string
	RegistryName string

	// Schema Configuration
	AutoRegisterSchemas bool
	SchemaCompatibility string

	// Caching Configuration
	CacheSize int
	CacheTTL  int // in seconds

	// Compression Configuration
	CompressionType string

	// Additional configuration options
	AdditionalConfig map[string]interface{}
}

// DefaultKafkaSerializerConfig returns a default configuration
func DefaultKafkaSerializerConfig() *KafkaSerializerConfig {
	return &KafkaSerializerConfig{
		Region:              "us-east-1",
		AutoRegisterSchemas: true,
		SchemaCompatibility: "BACKWARD",
		CacheSize:           100,
		CacheTTL:            3600, // 1 hour
		CompressionType:     "none",
		AdditionalConfig:    make(map[string]interface{}),
	}
}

// KafkaSerializer is a Kafka-specific serializer that mirrors the C# implementation
// It provides a high-level interface for serializing Kafka messages using AWS Glue Schema Registry
// NOTE: This serializer is NOT thread-safe. Each instance should be used by
// only one context/operation to comply with native library constraints.
type KafkaSerializer struct {
	coreSerializer    *gsrserde.Serializer
	formatFactory     SerializerFactory
	config            *KafkaSerializerConfig
	schemaNameStrategy common.SchemaNameStrategy
	closed            bool
}

// NewKafkaSerializer creates a new Kafka serializer instance
func NewKafkaSerializer(config *KafkaSerializerConfig) (*KafkaSerializer, error) {
	if config == nil {
		config = DefaultKafkaSerializerConfig()
	}

	// Create core serializer for GSR operations
	coreSerializer, err := gsrserde.NewSerializer()
	if err != nil {
		return nil, fmt.Errorf("failed to create core serializer: %w", err)
	}

	// Get format serializer factory
	formatFactory := GetSerializerFactory()

	return &KafkaSerializer{
		coreSerializer:     coreSerializer,
		formatFactory:      formatFactory,
		config:             config,
		schemaNameStrategy: &common.DefaultSchemaNameStrategy{},
		closed:             false,
	}, nil
}

// NewKafkaSerializerWithDefaults creates a new Kafka serializer with default configuration
func NewKafkaSerializerWithDefaults() (*KafkaSerializer, error) {
	return NewKafkaSerializer(DefaultKafkaSerializerConfig())
}

// Configure updates the serializer configuration
func (ks *KafkaSerializer) Configure(config *KafkaSerializerConfig) error {
	if ks.closed {
		return gsrserde.ErrClosed
	}

	if config == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	ks.config = config
	return nil
}

// Serialize serializes a message for Kafka using AWS Glue Schema Registry
// This method mirrors the C# GlueSchemaRegistryKafkaSerializer.Serialize method
func (ks *KafkaSerializer) Serialize(topic string, data interface{}) ([]byte, error) {
	if ks.closed {
		return nil, gsrserde.ErrClosed
	}

	// Handle nil data case (mirrors C# behavior)
	if data == nil {
		return nil, nil
	}

	// Create schema based on the data type
	schema, err := ks.createSchemaFromData(data, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema from data: %w", err)
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
	
	// Get the appropriate format serializer (mirrors C# factory.GetSerializer call)
	formatSerializer, err := ks.formatFactory.GetSerializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get serializer for format %s: %w", schema.DataFormat, err)
	}

	// Let the format serializer set additional schema info
	if err := formatSerializer.SetAdditionalSchemaInfo(data, schema); err != nil {
		return nil, fmt.Errorf("failed to set additional schema info: %w", err)
	}

	// Validate the object before serialization
	if err := formatSerializer.ValidateObject(data); err != nil {
		return nil, fmt.Errorf("data validation failed: %w", err)
	}

	// Serialize the message content (mirrors C# serializer.Serialize call)
	serializedData, err := formatSerializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize %s data: %w", dataFormat, err)
	}

	// Wrap with GSR header using transport name (topic)
	// This mirrors the C# Encode call with transport name
	encodedData, err := ks.coreSerializer.Encode(serializedData, topic, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to encode GSR data: %w", err)
	}

	return encodedData, nil
}

// SerializeWithSchema serializes a message with an explicit schema
// This provides more control over the schema used for serialization
func (ks *KafkaSerializer) SerializeWithSchema(topic string, data interface{}, schema *gsrserde.Schema) ([]byte, error) {
	if ks.closed {
		return nil, gsrserde.ErrClosed
	}

	// Handle nil data case
	if data == nil {
		return nil, nil
	}

	if schema == nil {
		return nil, gsrserde.ErrNilSchema
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
	
	// Get the appropriate format serializer
	formatSerializer, err := ks.formatFactory.GetSerializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get serializer for format %s: %w", schema.DataFormat, err)
	}

	// Validate the object before serialization
	if err := formatSerializer.ValidateObject(data); err != nil {
		return nil, fmt.Errorf("data validation failed: %w", err)
	}

	// Serialize the message content
	serializedData, err := formatSerializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize %s data: %w", dataFormat, err)
	}

	// Wrap with GSR header using transport name (topic)
	encodedData, err := ks.coreSerializer.Encode(serializedData, topic, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to encode GSR data: %w", err)
	}

	return encodedData, nil
}

// ValidateData validates that the provided data can be serialized
func (ks *KafkaSerializer) ValidateData(data interface{}) error {
	if ks.closed {
		return gsrserde.ErrClosed
	}

	if data == nil {
		return gsrserde.ErrNilData
	}

	// Create schema to determine data format
	schema, err := ks.createSchemaFromData(data, "")
	if err != nil {
		return fmt.Errorf("failed to create schema from data: %w", err)
	}

	// Create configuration from schema information
	configMap := make(map[string]interface{})
	
	// Convert schema DataFormat string to DataFormat enum
	dataFormat, err := stringToDataFormat(schema.DataFormat)
	if err != nil {
		return err
	}
	
	configMap[common.DataFormatTypeKey] = dataFormat
	
	// Create configuration object
	config := common.NewConfiguration(configMap)
	
	// Get the appropriate format serializer
	formatSerializer, err := ks.formatFactory.GetSerializer(config)
	if err != nil {
		return fmt.Errorf("failed to get serializer for format %s: %w", schema.DataFormat, err)
	}

	// Validate the object
	return formatSerializer.ValidateObject(data)
}

// GetSchemaFromData extracts schema information from the provided data
func (ks *KafkaSerializer) GetSchemaFromData(data interface{}) (*gsrserde.Schema, error) {
	if ks.closed {
		return nil, gsrserde.ErrClosed
	}

	if data == nil {
		return nil, gsrserde.ErrNilData
	}

	return ks.createSchemaFromData(data, "")
}

// createSchemaFromData creates a schema based on the data type
// This determines the appropriate data format based on the Go type
func (ks *KafkaSerializer) createSchemaFromData(data interface{}, topic string) (*gsrserde.Schema, error) {
	if data == nil {
		return nil, gsrserde.ErrNilData
	}

	// Create initial schema
	schema := &gsrserde.Schema{
		Name:           "", // Will be set by format serializer
		Definition:     "", // Will be set by format serializer
		DataFormat:     "", // Will be determined below
		AdditionalInfo: "", // Will be set by format serializer
	}

	// Determine data format based on type
	// This mirrors the C# logic for detecting data format
	switch data.(type) {
	case map[string]interface{}:
		// AVRO records are represented as map[string]interface{} in Go
		schema.DataFormat = "AVRO"
	default:
		// Check if it's a protobuf message
		if _, ok := data.(interface{ ProtoMessage() }); ok {
			schema.DataFormat = "PROTOBUF"
		} else {
			// For other types that need JSON serialization, use JSON
			// Note: JSON serializer is not yet implemented
			schema.DataFormat = "JSON"
		}
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
	
	// Get the format serializer to populate schema details
	formatSerializer, err := ks.formatFactory.GetSerializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get serializer for format %s: %w", schema.DataFormat, err)
	}

	// Get schema definition from the data
	definition, err := formatSerializer.GetSchemaDefinition(data)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema definition: %w", err)
	}
	schema.Definition = definition

	// Set additional schema info
	if err := formatSerializer.SetAdditionalSchemaInfo(data, schema); err != nil {
		return nil, fmt.Errorf("failed to set additional schema info: %w", err)
	}

	// Generate schema name using the naming strategy and topic
	if topic != "" && ks.schemaNameStrategy != nil {
		schemaName := ks.schemaNameStrategy.GetSchemaName(data, topic)
		if schemaName != "" {
			schema.Name = schemaName
		}
	}

	return schema, nil
}

// GetConfiguration returns the current configuration
func (ks *KafkaSerializer) GetConfiguration() *KafkaSerializerConfig {
	// Return a copy to prevent external modification
	configCopy := *ks.config
	configCopy.AdditionalConfig = make(map[string]interface{})
	for k, v := range ks.config.AdditionalConfig {
		configCopy.AdditionalConfig[k] = v
	}

	return &configCopy
}

// Close releases all resources associated with the serializer
func (ks *KafkaSerializer) Close() error {
	if ks.closed {
		return nil
	}

	ks.closed = true

	// Close core serializer
	if ks.coreSerializer != nil {
		err := ks.coreSerializer.Close()
		if err != nil {
			return fmt.Errorf("failed to close core serializer: %w", err)
		}
	}

	// No need to clear factory cache since we don't cache instances anymore

	return nil
}

// IsClosed returns whether the serializer is closed
func (ks *KafkaSerializer) IsClosed() bool {
	return ks.closed
}
