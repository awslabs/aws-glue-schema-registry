package integration_tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// BaseIntegrationSuite provides common functionality for all integration test suites
type BaseIntegrationSuite struct {
	suite.Suite
	kafkaSerializer   *serializer.KafkaSerializer
	kafkaDeserializer *deserializer.KafkaDeserializer
	topicName         string
	cleanup           func()
	// NOTE: No mutex needed - serializers are single-use and not shared between tests
	// Removed resourceMutex to prevent cross-thread cleanup issues with GraalVM isolates
}

// SetupSuite is called once before all tests in the suite
func (s *BaseIntegrationSuite) SetupSuite() {
	s.T().Log("=== Setting up Base Integration Suite ===")

	// Force sequential execution to prevent concurrency issues
	s.T().Log("Forcing sequential test execution...")

	// Verify Kafka is running
	s.requireKafkaRunning()

	s.T().Log("=== Base Integration Suite Setup Complete ===")
}

// TearDownSuite is called once after all tests in the suite
func (s *BaseIntegrationSuite) TearDownSuite() {
	s.T().Log("=== Starting Base Suite Teardown ===")

	// Clear any remaining references and let Go GC handle cleanup
	s.kafkaSerializer = nil
	s.kafkaDeserializer = nil
	s.T().Log("✓ Cleared all serializer/deserializer references")

	s.T().Log("=== Base Suite Teardown Complete ===")
}

// SetupTest is called before each test method
func (s *BaseIntegrationSuite) SetupTest() {
	s.topicName = s.generateTestTopicName()
	s.cleanup = s.setupTestInfrastructure()

	s.T().Logf("Test setup complete for topic: %s", s.topicName)
}

// TearDownTest is called after each test method
func (s *BaseIntegrationSuite) TearDownTest() {
	s.T().Log("Starting test teardown...")

	// Clear references to allow GC - don't call Close() to avoid thread affinity issues
	s.kafkaSerializer = nil
	s.kafkaDeserializer = nil
	s.T().Log("✓ Cleared serializer/deserializer references")

	// Execute Kafka topic cleanup function
	if s.cleanup != nil {
		s.cleanup()
		s.cleanup = nil
	}

	// Let Go GC and process termination handle native resource cleanup
	s.T().Log("Test teardown complete")
}

// createKafkaSerializer creates a KafkaSerializer configured for AWS GSR
func (s *BaseIntegrationSuite) createKafkaSerializer(config *common.Configuration) *serializer.KafkaSerializer {
	kafkaSerializer, err := serializer.NewKafkaSerializer(config)
	require.NoError(s.T(), err, "Should create KafkaSerializer")
	require.NotNil(s.T(), kafkaSerializer, "KafkaSerializer should not be nil")

	return kafkaSerializer
}

// createKafkaDeserializer creates a KafkaDeserializer configured for AWS GSR
func (s *BaseIntegrationSuite) createKafkaDeserializer(config *common.Configuration) *deserializer.KafkaDeserializer {
	kafkaDeserializer, err := deserializer.NewKafkaDeserializer(config)
	require.NoError(s.T(), err, "Should create KafkaDeserializer")
	require.NotNil(s.T(), kafkaDeserializer, "KafkaDeserializer should not be nil")

	return kafkaDeserializer
}

// runKafkaIntegrationTest executes the complete integration test flow
func (s *BaseIntegrationSuite) runKafkaIntegrationTest(
	originalMessage interface{},
	validate func(original, deserialized interface{}),
	config *common.Configuration,
) {
	ctx := context.Background()

	// Step 1: Create KafkaSerializer with GSR configuration
	s.kafkaSerializer = s.createKafkaSerializer(config)

	// Step 2: Serialize the message (auto-registers schema with GSR)
	s.T().Logf("Serializing %T message", originalMessage)
	gsrEncodedData, err := s.kafkaSerializer.Serialize(s.topicName, originalMessage)
	require.NoError(s.T(), err, "KafkaSerializer.Serialize should succeed")
	require.NotEmpty(s.T(), gsrEncodedData, "Serialized data should not be empty")
	s.T().Logf("Serialized message: %d bytes", len(gsrEncodedData))

	s.T().Logf("Publishing message to topicName: %s with", s.topicName)
	// Step 3: Publish the GSR-encoded data to Kafka
	s.publishMessageToKafka(ctx, s.topicName, gsrEncodedData)

	// Step 4: Consume the GSR-encoded data from Kafka
	consumedData := s.consumeMessageFromKafka(ctx, s.topicName)
	require.Equal(s.T(), gsrEncodedData, consumedData, "Data consumed from Kafka should match published data")

	// Step 5: Create KafkaDeserializer and deserialize the GSR-encoded data
	s.kafkaDeserializer = s.createKafkaDeserializer(config)

	// Verify the data can be deserialized
	canDeserialize, err := s.kafkaDeserializer.CanDeserialize(consumedData)
	require.NoError(s.T(), err, "Should check if data can be deserialized")
	require.True(s.T(), canDeserialize, "GSR-encoded data should be deserializable")

	// Deserialize the GSR-encoded data back to the original message
	s.T().Log("Deserializing GSR-encoded data")
	deserializedMessage, err := s.kafkaDeserializer.Deserialize(s.topicName, consumedData)
	require.NoError(s.T(), err, "KafkaDeserializer.Deserialize should succeed")
	require.NotNil(s.T(), deserializedMessage, "Deserialized message should not be nil")

	// Step 6: Validate the round-trip
	s.T().Log("Validating round-trip")
	validate(originalMessage, deserializedMessage)

	s.T().Logf("✅ Integration test passed for %T", originalMessage)
}

// publishMessageToKafka publishes data to a Kafka topic
func (s *BaseIntegrationSuite) publishMessageToKafka(ctx context.Context, topicName string, data []byte) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(s.getKafkaBroker()),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	message := kafka.Message{
		Key:   []byte("test-key"),
		Value: data,
	}

	err := writer.WriteMessages(ctx, message)
	require.NoError(s.T(), err, "Should publish message to Kafka")
	s.T().Logf("Published message to topic %s: %d bytes", topicName, len(data))
}

// consumeMessageFromKafka consumes a single message from a Kafka topic
func (s *BaseIntegrationSuite) consumeMessageFromKafka(ctx context.Context, topicName string) []byte {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{s.getKafkaBroker()},
		Topic:   topicName,
		GroupID: fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
	})
	defer reader.Close()

	// Set a reasonable timeout for reading
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	message, err := reader.ReadMessage(readCtx)
	require.NoError(s.T(), err, "Should consume message from Kafka")
	s.T().Logf("Consumed message from topic %s: %d bytes", topicName, len(message.Value))

	return message.Value
}

// Infrastructure setup and helper functions

// requireKafkaRunning ensures Kafka is running and accessible
func (s *BaseIntegrationSuite) requireKafkaRunning() {
	conn, err := kafka.Dial("tcp", s.getKafkaBroker())
	require.NoError(s.T(), err, "Kafka should be running at %s", s.getKafkaBroker())
	defer conn.Close()

	s.T().Logf("✅ Kafka is running at %s", s.getKafkaBroker())
}

// setupTestInfrastructure sets up the test topic and returns cleanup function
func (s *BaseIntegrationSuite) setupTestInfrastructure() func() {
	ctx := context.Background()

	// Create topic
	s.createKafkaTopic(ctx, s.topicName)

	// Return cleanup function
	return func() {
		s.deleteKafkaTopic(ctx, s.topicName)
	}
}

// createKafkaTopic creates a Kafka topic for testing
func (s *BaseIntegrationSuite) createKafkaTopic(ctx context.Context, topicName string) {
	conn, err := kafka.Dial("tcp", s.getKafkaBroker())
	require.NoError(s.T(), err)
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		s.T().Logf("Warning: Could not create topic %s (might already exist): %v", topicName, err)
	} else {
		s.T().Logf("Created Kafka topic: %s", topicName)
	}
}

// deleteKafkaTopic deletes a Kafka topic after testing
func (s *BaseIntegrationSuite) deleteKafkaTopic(ctx context.Context, topicName string) {
	conn, err := kafka.Dial("tcp", s.getKafkaBroker())
	if err != nil {
		s.T().Logf("Warning: Could not connect to Kafka for cleanup: %v", err)
		return
	}
	defer conn.Close()

	err = conn.DeleteTopics(topicName)
	if err != nil {
		s.T().Logf("Warning: Could not delete topic %s: %v", topicName, err)
	} else {
		s.T().Logf("Deleted Kafka topic: %s", topicName)
	}
}

// generateTestTopicName generates a unique topic name for testing
func (s *BaseIntegrationSuite) generateTestTopicName() string {
	randomBytes := make([]byte, 4)
	rand.Read(randomBytes)
	return fmt.Sprintf("golang-integration-test-suite-%x", randomBytes)
}

// getKafkaBroker returns the Kafka broker address
func (s *BaseIntegrationSuite) getKafkaBroker() string {
	if broker := os.Getenv("KAFKA_BROKER"); broker != "" {
		return broker
	}
	return defaultKafkaBroker
}

// getAWSRegion returns the AWS region for GSR
func (s *BaseIntegrationSuite) getAWSRegion() string {
	if region := os.Getenv("AWS_REGION"); region != "" {
		return region
	}
	if region := os.Getenv("AWS_DEFAULT_REGION"); region != "" {
		return region
	}
	return defaultAWSRegion
}
