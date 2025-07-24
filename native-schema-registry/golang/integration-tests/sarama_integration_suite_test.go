package integration_tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// SaramaIntegrationSuite tests protobuf integration with Sarama and AWS GSR
type SaramaIntegrationSuite struct {
	BaseIntegrationSuite
}

// TestSaramaProtobufIntegration tests the complete end-to-end flow using Sarama
func (s *SaramaIntegrationSuite) TestSaramaProtobufIntegration() {
	if s.shouldSkipIntegrationTests() {
		s.T().Skip("Skipping integration test")
	}

	s.T().Log("--- Starting Sarama Protobuf Integration Test ---")

	// Create protobuf test message
	message := &testpb.TestMessage{
		Id:    "sarama-test-789",
		Name:  "Sarama Integration Test",
		Age:   28,
		Email: "sarama@example.com",
		Tags:  []string{"integration", "test", "sarama", "protobuf", "gsr", "pure-go"},
	}

	// Extract the message descriptor using protobuf reflection
	messageDescriptor := message.ProtoReflect().Descriptor()

	// Create Protobuf configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
	}
	config := common.NewConfiguration(configMap)

	// Run the integration test with custom Sarama transport
	s.runSaramaIntegrationTest(message, s.validateProtobufMessage, config)

	s.T().Log("--- Sarama Protobuf Integration Test Complete ---")
}

// runSaramaIntegrationTest executes the integration test using Sarama for transport
func (s *SaramaIntegrationSuite) runSaramaIntegrationTest(
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

	// Step 3: Publish the GSR-encoded data to Kafka using Sarama
	s.T().Logf("Publishing message to topic: %s using Sarama", s.topicName)
	s.publishMessageToKafkaSarama(ctx, s.topicName, gsrEncodedData)

	// Step 4: Consume the GSR-encoded data from Kafka using Sarama
	consumedData := s.consumeMessageFromKafkaSarama(ctx, s.topicName)
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

	s.T().Logf("✅ Sarama integration test passed for %T", originalMessage)
}

// publishMessageToKafkaSarama publishes data to Kafka using Sarama
func (s *SaramaIntegrationSuite) publishMessageToKafkaSarama(ctx context.Context, topicName string, data []byte) {
	// Create Sarama producer configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer([]string{s.getKafkaBroker()}, config)
	require.NoError(s.T(), err, "Should create Sarama producer")
	defer producer.Close()

	// Create message
	message := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("sarama-test-key"),
		Value: sarama.ByteEncoder(data),
	}

	// Send message
	partition, offset, err := producer.SendMessage(message)
	require.NoError(s.T(), err, "Should send message with Sarama")

	s.T().Logf("Message sent to partition %d at offset %d using Sarama", partition, offset)
	s.T().Logf("Published message to topic %s using Sarama: %d bytes", topicName, len(data))
}

// consumeMessageFromKafkaSarama consumes a single message from Kafka using Sarama
func (s *SaramaIntegrationSuite) consumeMessageFromKafkaSarama(ctx context.Context, topicName string) []byte {
	// Create Sarama consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{s.getKafkaBroker()}, config)
	require.NoError(s.T(), err, "Should create Sarama consumer")
	defer consumer.Close()

	// Get available partitions
	partitions, err := consumer.Partitions(topicName)
	require.NoError(s.T(), err, "Should get partitions for topic")
	require.NotEmpty(s.T(), partitions, "Topic should have partitions")

	// Consume from the first partition
	partitionConsumer, err := consumer.ConsumePartition(topicName, partitions[0], sarama.OffsetOldest)
	require.NoError(s.T(), err, "Should create partition consumer")
	defer partitionConsumer.Close()

	// Consume message with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			s.T().Fatal("Consume timeout using Sarama")
		case err := <-partitionConsumer.Errors():
			require.NoError(s.T(), err, "Consumer error from Sarama")
		case msg := <-partitionConsumer.Messages():
			s.T().Logf("Received message from partition %d at offset %d using Sarama", 
				msg.Partition, msg.Offset)
			s.T().Logf("Consumed message from topic %s using Sarama: %d bytes", topicName, len(msg.Value))
			return msg.Value
		}
	}
}

// validateProtobufMessage validates protobuf message round-trip
func (s *SaramaIntegrationSuite) validateProtobufMessage(original, deserialized interface{}) {
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

	s.T().Logf("✅ Sarama protobuf message validation passed")
}

// convertDynamicToTestMessage converts a dynamic protobuf message to a concrete TestMessage
func (s *SaramaIntegrationSuite) convertDynamicToTestMessage(dynamic proto.Message) (*testpb.TestMessage, error) {
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
func (s *SaramaIntegrationSuite) shouldSkipIntegrationTests() bool {
	return os.Getenv("SKIP_INTEGRATION_TESTS") == "true"
}

// TestSaramaIntegrationSuite runs the Sarama integration test suite
func TestSaramaIntegrationSuite(t *testing.T) {
	suite.Run(t, new(SaramaIntegrationSuite))
}
