package gsrserde

import (
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
)

// KafkaDeserializer type alias for easy access from main package
type KafkaDeserializer = deserializer.KafkaDeserializer

// KafkaDeserializerConfig type alias for easy access from main package
type KafkaDeserializerConfig = deserializer.KafkaDeserializerConfig

// NewKafkaDeserializer creates a new Kafka deserializer instance
// This is a convenience function that mirrors the C# implementation structure
func NewKafkaDeserializer(config *KafkaDeserializerConfig) (*KafkaDeserializer, error) {
	return deserializer.NewKafkaDeserializer(config)
}

// NewKafkaDeserializerWithDefaults creates a new Kafka deserializer with default configuration
// This is a convenience function that mirrors the C# implementation structure
func NewKafkaDeserializerWithDefaults() (*KafkaDeserializer, error) {
	return deserializer.NewKafkaDeserializerWithDefaults()
}

// DefaultKafkaDeserializerConfig returns a default configuration
// This is a convenience function that mirrors the C# implementation structure
func DefaultKafkaDeserializerConfig() *KafkaDeserializerConfig {
	return deserializer.DefaultKafkaDeserializerConfig()
}
