package gsrserde

import (
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// KafkaDeserializer type alias for easy access from main package
type KafkaDeserializer = deserializer.Deserializer

// KafkaSerializer type alias for easy access from main package
type KafkaSerializer = serializer.Serializer

// NewKafkaDeserializer creates a new Kafka deserializer instance
func NewKafkaDeserializer(config *common.Configuration) (*KafkaDeserializer, error) {
	return deserializer.NewDeserializer(config)
}

// NewKafkaSerializer creates a new Kafka serializer instance
func NewKafkaSerializer(config *common.Configuration) (*KafkaSerializer, error) {
	return serializer.NewSerializer(config)
}
