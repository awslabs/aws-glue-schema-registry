package gsrserde

import (
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// KafkaDeserializer type alias for easy access from main package
type KafkaDeserializer = deserializer.KafkaDeserializer

// KafkaSerializer type alias for easy access from main package
type KafkaSerializer = serializer.KafkaSerializer

// NewKafkaDeserializer creates a new Kafka deserializer instance
// This is a convenience function that mirrors the C# implementation structure
func NewKafkaDeserializer(config *common.Configuration) (*KafkaDeserializer, error) {
	return deserializer.NewKafkaDeserializer(config)
}

// NewKafkaSerializer creates a new Kafka serializer instance
// This is a convenience function that mirrors the C# implementation structure
func NewKafkaSerializer(config *common.Configuration) (*KafkaSerializer, error) {
	return serializer.NewKafkaSerializer(config)
}
