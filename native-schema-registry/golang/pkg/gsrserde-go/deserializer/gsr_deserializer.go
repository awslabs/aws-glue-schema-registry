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

// Deserializer is a high-level deserializer that mirrors the C# implementation
// It provides a high-level interface for deserializing messages using AWS Glue Schema Registry
// NOTE: This deserializer is NOT thread-safe. Each instance should be used by
// only one context/operation to comply with native library constraints.
type Deserializer struct {
	coreDeserializer   *gsrserde.Deserializer
	formatDeserializer DataFormatDeserializer
	formatFactory      DeserializerFactory
	config             *common.Configuration
	closed             bool
}

// NewDeserializer creates a new deserializer instance
func NewDeserializer(config *common.Configuration) (*Deserializer, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}

	// Create core deserializer for GSR operations
	coreDeserializer, err := gsrserde.NewDeserializer()
	if err != nil {
		return nil, fmt.Errorf("failed to create core deserializer: %w", err)
	}

	// Get format deserializer factory
	formatFactory := GetDeserializerFactory()

	// Create format deserializer based on configuration
	formatDeserializer, err := formatFactory.GetDeserializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create format deserializer: %w", err)
	}

	return &Deserializer{
		coreDeserializer:   coreDeserializer,
		formatDeserializer: formatDeserializer,
		formatFactory:      formatFactory,
		config:             config,
		closed:             false,
	}, nil
}

// Deserialize deserializes a message using AWS Glue Schema Registry
// This method mirrors the C# GlueSchemaRegistryKafkaDeserializer.Deserialize method
func (d *Deserializer) Deserialize(topic string, data []byte) (interface{}, error) {
	if d.closed {
		return nil, gsrserde.ErrClosed
	}

	// Handle nil data case (mirrors C# behavior)
	if data == nil {
		return nil, nil
	}

	// Check if data can be decoded (mirrors C# CanDecode check)
	canDecode, err := d.coreDeserializer.CanDecode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to check if data can be decoded: %w", err)
	}

	if !canDecode {
		return nil, fmt.Errorf("byte data cannot be decoded: data does not contain GSR header")
	}

	// Decode the GSR-wrapped bytes (mirrors C# Decode call)
	decodedBytes, err := d.coreDeserializer.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode GSR data: %w", err)
	}

	// Extract schema information (mirrors C# DecodeSchema call)
	schema, err := d.coreDeserializer.DecodeSchema(data)
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
	formatDeserializer, err := d.formatFactory.GetDeserializer(config)
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
func (d *Deserializer) CanDeserialize(data []byte) (bool, error) {
	if d.closed {
		return false, gsrserde.ErrClosed
	}

	if data == nil {
		return false, nil
	}

	return d.coreDeserializer.CanDecode(data)
}

// GetSchema extracts schema information from the data without deserializing the message
func (d *Deserializer) GetSchema(data []byte) (*gsrserde.Schema, error) {
	if d.closed {
		return nil, gsrserde.ErrClosed
	}

	if data == nil {
		return nil, gsrserde.ErrNilData
	}

	return d.coreDeserializer.DecodeSchema(data)
}

// GetConfiguration returns the current configuration
func (d *Deserializer) GetConfiguration() *common.Configuration {
	return d.config
}

// Close releases all resources associated with the deserializer
func (d *Deserializer) Close() error {
	if d.closed {
		return nil
	}

	d.closed = true

	// Close core deserializer
	if d.coreDeserializer != nil {
		err := d.coreDeserializer.Close()
		if err != nil {
			return fmt.Errorf("failed to close core deserializer: %w", err)
		}
	}

	// No need to clear factory cache since we don't cache instances anymore

	return nil
}

// IsClosed returns whether the deserializer is closed
func (d *Deserializer) IsClosed() bool {
	return d.closed
}
