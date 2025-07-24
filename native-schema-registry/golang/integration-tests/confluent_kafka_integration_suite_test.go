package integration_tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// ConfluentKafkaIntegrationSuite tests protobuf integration with confluent-kafka-go and AWS GSR
type ConfluentKafkaIntegrationSuite struct {
	BaseIntegrationSuite
}

// TestConfluentKafkaProtobufIntegration tests the complete end-to-end flow using confluent-kafka-go
func (s *ConfluentKafkaIntegrationSuite) TestConfluentKafkaProtobufIntegration() {
	if s.shouldSkipIntegrationTests() {
		s.T().Skip("Skipping integration test")
	}

	s.T().Log("--- Starting Confluent Kafka Protobuf Integration Test ---")

	// Create protobuf test message
	message := &testpb.TestMessage{
		Id:    "confluent-kafka-test-456",
		Name:  "Confluent Kafka Integration Test",
		Age:   35,
		Email: "confluent.kafka@example.com",
		Tags:  []string{"integration", "test", "confluent-kafka-go", "protobuf", "gsr"},
	}

	// Extract the message descriptor using protobuf reflection
	messageDescriptor := message.ProtoReflect().Descriptor()

	// Create Protobuf configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
	}
	config := common.NewConfiguration(configMap)

	// Run the integration test with custom confluent-kafka-go transport
	s.runConfluentKafkaIntegrationTest(message, s.validateProtobufMessage, config)

	s.T().Log("--- Confluent Kafka Protobuf Integration Test Complete ---")
}

// runConfluentKafkaIntegrationTest executes the integration test using confluent-kafka-go for transport
func (s *ConfluentKafkaIntegrationSuite) runConfluentKafkaIntegrationTest(
	originalMessage interface{},
	validate func(original, deserialized interface{}),
	config *common.Configuration,
) {
	ctx := context.Background()

	// Step 1: Create Serializer with GSR configuration
	s.gsr_serializer = s.createSerializer(config)

	// Step 2: Serialize the message (auto-registers schema with GSR)
	s.T().Logf("Serializing %T message", originalMessage)
	gsrEncodedData, err := s.gsr_serializer.Serialize(s.topicName, originalMessage)
	require.NoError(s.T(), err, "Serializer.Serialize should succeed")
	require.NotEmpty(s.T(), gsrEncodedData, "Serialized data should not be empty")
	s.T().Logf("Serialized message: %d bytes", len(gsrEncodedData))

	// Step 3: Publish the GSR-encoded data to Kafka using confluent-kafka-go
	s.T().Logf("Publishing message to topic: %s using confluent-kafka-go", s.topicName)
	s.publishMessageToKafkaConfluentKafka(ctx, s.topicName, gsrEncodedData)

	// Step 4: Consume the GSR-encoded data from Kafka using confluent-kafka-go
	consumedData := s.consumeMessageFromKafkaConfluentKafka(ctx, s.topicName)
	require.Equal(s.T(), gsrEncodedData, consumedData, "Data consumed from Kafka should match published data")

	// Step 5: Create Deserializer and deserialize the GSR-encoded data
	s.gsr_deserializer = s.createDeserializer(config)

	// Verify the data can be deserialized
	canDeserialize, err := s.gsr_deserializer.CanDeserialize(consumedData)
	require.NoError(s.T(), err, "Should check if data can be deserialized")
	require.True(s.T(), canDeserialize, "GSR-encoded data should be deserializable")

	// Deserialize the GSR-encoded data back to the original message
	s.T().Log("Deserializing GSR-encoded data")
	deserializedMessage, err := s.gsr_deserializer.Deserialize(s.topicName, consumedData)
	require.NoError(s.T(), err, "Deserializer.Deserialize should succeed")
	require.NotNil(s.T(), deserializedMessage, "Deserialized message should not be nil")

	// Step 6: Validate the round-trip
	s.T().Log("Validating round-trip")
	validate(originalMessage, deserializedMessage)

	s.T().Logf("✅ Confluent Kafka integration test passed for %T", originalMessage)
}

// publishMessageToKafkaConfluentKafka publishes data to Kafka using confluent-kafka-go
func (s *ConfluentKafkaIntegrationSuite) publishMessageToKafkaConfluentKafka(ctx context.Context, topicName string, data []byte) {
	// Create producer configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers": s.getKafkaBroker(),
		"acks":              "all",
		"retries":           3,
		"max.in.flight.requests.per.connection": 1,
		"enable.idempotence": true,
	}

	producer, err := kafka.NewProducer(config)
	require.NoError(s.T(), err, "Should create confluent-kafka-go producer")
	defer producer.Close()

	// Create message
	topic := topicName
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte("confluent-test-key"),
		Value: data,
	}

	// Send message
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err = producer.Produce(message, deliveryChan)
	require.NoError(s.T(), err, "Should produce message with confluent-kafka-go")

	// Wait for delivery report
	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		require.NoError(s.T(), m.TopicPartition.Error, "Message delivery should succeed")
		s.T().Logf("Message delivered to partition %d at offset %v using confluent-kafka-go", 
			m.TopicPartition.Partition, m.TopicPartition.Offset)
	case <-time.After(30 * time.Second):
		s.T().Fatal("Delivery timeout using confluent-kafka-go")
	}

	s.T().Logf("Published message to topic %s using confluent-kafka-go: %d bytes", topicName, len(data))
}

// consumeMessageFromKafkaConfluentKafka consumes a single message from Kafka using confluent-kafka-go
func (s *ConfluentKafkaIntegrationSuite) consumeMessageFromKafkaConfluentKafka(ctx context.Context, topicName string) []byte {
	// Create consumer configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers": s.getKafkaBroker(),
		"group.id":          fmt.Sprintf("confluent-test-group-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	require.NoError(s.T(), err, "Should create confluent-kafka-go consumer")
	defer consumer.Close()

	// Subscribe to topic
	err = consumer.SubscribeTopics([]string{topicName}, nil)
	require.NoError(s.T(), err, "Should subscribe to topic with confluent-kafka-go")

	// Consume message with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			s.T().Fatal("Consume timeout using confluent-kafka-go")
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				require.NoError(s.T(), err, "Should read message with confluent-kafka-go")
			}

			s.T().Logf("Received message from partition %d at offset %v using confluent-kafka-go", 
				msg.TopicPartition.Partition, msg.TopicPartition.Offset)
			s.T().Logf("Consumed message from topic %s using confluent-kafka-go: %d bytes", topicName, len(msg.Value))
			return msg.Value
		}
	}
}

// validateProtobufMessage validates protobuf message round-trip
func (s *ConfluentKafkaIntegrationSuite) validateProtobufMessage(original, deserialized interface{}) {
	origMsg := original.(*testpb.TestMessage)

	// The deserializer correctly returns *dynamicpb.Message, so we need to convert it
	protoMessage, ok := deserialized.(proto.Message)
	require.True(s.T(), ok, "Deserialized message should be a proto.Message, got %T", deserialized)

	deserMsg, err := s.convertDynamicToTestMessage(protoMessage)
	require.NoError(s.T(), err, "Should convert dynamic message to TestMessage")
	require.NotNil(s.T(), deserMsg, "Converted message should not be nil")

	s.T().Logf("Original: %v", origMsg)
	s.T().Logf("Deserialized (after conversion): %v", deserMsg)

	// Validate all fields match
	assert.Equal(s.T(), origMsg.GetId(), deserMsg.GetId(), "ID should match")
	assert.Equal(s.T(), origMsg.GetName(), deserMsg.GetName(), "Name should match")
	assert.Equal(s.T(), origMsg.GetAge(), deserMsg.GetAge(), "Age should match")
	assert.Equal(s.T(), origMsg.GetEmail(), deserMsg.GetEmail(), "Email should match")
	assert.Equal(s.T(), origMsg.GetTags(), deserMsg.GetTags(), "Tags should match")

	s.T().Logf("✅ Confluent Kafka protobuf message validation passed")
}

// convertDynamicToTestMessage converts a dynamic protobuf message to a concrete TestMessage
func (s *ConfluentKafkaIntegrationSuite) convertDynamicToTestMessage(dynamic proto.Message) (*testpb.TestMessage, error) {
	// Serialize the dynamic message to bytes
	data, err := proto.Marshal(dynamic)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal dynamic message: %w", err)
	}

	// Deserialize into the concrete TestMessage type
	concrete := &testpb.TestMessage{}
	if err := proto.Unmarshal(data, concrete); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to TestMessage: %w", err)
	}

	return concrete, nil
}

// shouldSkipIntegrationTests checks if integration tests should be skipped
func (s *ConfluentKafkaIntegrationSuite) shouldSkipIntegrationTests() bool {
	return os.Getenv("SKIP_INTEGRATION_TESTS") == "true"
}

// TestConfluentKafkaIntegrationSuite runs the Confluent Kafka integration test suite
func TestConfluentKafkaIntegrationSuite(t *testing.T) {
	suite.Run(t, new(ConfluentKafkaIntegrationSuite))
}
