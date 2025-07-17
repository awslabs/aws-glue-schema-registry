package integration_tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

const (
	defaultKafkaBroker = "localhost:9092"
	defaultAWSRegion   = "us-east-1"
	testRegistryName   = "golang-integration-test-registry"
)

// TestKafkaSerializerDeserializerIntegration tests the complete end-to-end flow:
// KafkaSerializer -> AWS GSR -> Kafka -> Consumer -> KafkaDeserializer
func TestKafkaSerializerDeserializerIntegration(t *testing.T) {
	// Skip if running in CI without AWS credentials
	if os.Getenv("SKIP_INTEGRATION_TESTS") == "true" {
		t.Skip("Skipping integration test")
	}

	// Check prerequisites
	requireKafkaRunning(t)

	// Generate unique topic for this test
	topicName := generateTestTopicName()
	
	// Setup test infrastructure
	ctx := context.Background()
	cleanup := setupTestInfrastructure(t, ctx, topicName)
	defer cleanup()

	// Test cases with different protobuf messages - start with simple message first
	testCases := []struct {
		name     string
		message  proto.Message
		validate func(t *testing.T, original, deserialized proto.Message)
	}{
		{
			name: "TestMessage",
			message: &testpb.TestMessage{
				Id:    "test-123",
				Name:  "Integration Test",
				Age:   42,
				Email: "test@example.com",
				Tags:  []string{"integration", "test", "protobuf"},
			},
			validate: validateTestMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runKafkaSerializerIntegrationTest(t, ctx, topicName, tc.message, tc.validate)
		})
	}
}

// runKafkaSerializerIntegrationTest executes the complete integration test flow
func runKafkaSerializerIntegrationTest(
	t *testing.T,
	ctx context.Context,
	topicName string,
	originalMessage proto.Message,
	validate func(t *testing.T, original, deserialized proto.Message),
) {
	// Step 1: Create KafkaSerializer with GSR configuration
	kafkaSerializer := createKafkaSerializer(t)
	defer kafkaSerializer.Close()

	// Step 2: Serialize the protobuf message (auto-registers schema with GSR)
	t.Logf("Serializing %T message", originalMessage)
	gsrEncodedData, err := kafkaSerializer.Serialize(topicName, originalMessage)
	require.NoError(t, err, "KafkaSerializer.Serialize should succeed")
	require.NotEmpty(t, gsrEncodedData, "Serialized data should not be empty")
	t.Logf("Serialized message: %d bytes", len(gsrEncodedData))

	// Step 3: Publish the GSR-encoded data to Kafka
	publishMessageToKafka(t, ctx, topicName, gsrEncodedData)

	// Step 4: Consume the GSR-encoded data from Kafka
	consumedData := consumeMessageFromKafka(t, ctx, topicName)
	require.Equal(t, gsrEncodedData, consumedData, "Data consumed from Kafka should match published data")

	// Step 5: Create KafkaDeserializer and deserialize the GSR-encoded data
	kafkaDeserializer := createKafkaDeserializer(t)
	defer kafkaDeserializer.Close()

	// Verify the data can be deserialized
	canDeserialize, err := kafkaDeserializer.CanDeserialize(consumedData)
	require.NoError(t, err, "Should check if data can be deserialized")
	require.True(t, canDeserialize, "GSR-encoded data should be deserializable")

	// Deserialize the GSR-encoded data back to the original message
	t.Log("Deserializing GSR-encoded data")
	deserializedMessage, err := kafkaDeserializer.Deserialize(topicName, consumedData)
	require.NoError(t, err, "KafkaDeserializer.Deserialize should succeed")
	require.NotNil(t, deserializedMessage, "Deserialized message should not be nil")

	// Convert to proto.Message for validation
	protoMessage, ok := deserializedMessage.(proto.Message)
	require.True(t, ok, "Deserialized message should be a proto.Message, got %T", deserializedMessage)

	// Step 6: Validate the round-trip
	t.Log("Validating round-trip")
	validate(t, originalMessage, protoMessage)

	t.Logf("✅ Integration test passed for %T", originalMessage)
}

// createKafkaSerializer creates a KafkaSerializer configured for AWS GSR
func createKafkaSerializer(t *testing.T) *serializer.KafkaSerializer {
	config := &serializer.KafkaSerializerConfig{
		Region:              getAWSRegion(),
		RegistryName:        testRegistryName,
		AutoRegisterSchemas: true,
		SchemaCompatibility: "BACKWARD",
		CacheSize:           100,
		CacheTTL:            3600,
		CompressionType:     "none",
		AdditionalConfig:    make(map[string]interface{}),
	}

	kafkaSerializer, err := serializer.NewKafkaSerializer(config)
	require.NoError(t, err, "Should create KafkaSerializer")
	require.NotNil(t, kafkaSerializer, "KafkaSerializer should not be nil")

	return kafkaSerializer
}

// createKafkaDeserializer creates a KafkaDeserializer configured for AWS GSR
func createKafkaDeserializer(t *testing.T) *deserializer.KafkaDeserializer {
	config := &deserializer.KafkaDeserializerConfig{
		Region:           getAWSRegion(),
		RegistryName:     testRegistryName,
		CacheSize:        100,
		CacheTTL:         3600,
		CompressionType:  "none",
		AdditionalConfig: make(map[string]interface{}),
	}

	kafkaDeserializer, err := deserializer.NewKafkaDeserializer(config)
	require.NoError(t, err, "Should create KafkaDeserializer")
	require.NotNil(t, kafkaDeserializer, "KafkaDeserializer should not be nil")

	return kafkaDeserializer
}

// publishMessageToKafka publishes data to a Kafka topic
func publishMessageToKafka(t *testing.T, ctx context.Context, topicName string, data []byte) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(getKafkaBroker()),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	message := kafka.Message{
		Key:   []byte("test-key"),
		Value: data,
	}

	err := writer.WriteMessages(ctx, message)
	require.NoError(t, err, "Should publish message to Kafka")
	t.Logf("Published message to topic %s: %d bytes", topicName, len(data))
}

// consumeMessageFromKafka consumes a single message from a Kafka topic
func consumeMessageFromKafka(t *testing.T, ctx context.Context, topicName string) []byte {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{getKafkaBroker()},
		Topic:   topicName,
		GroupID: fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
	})
	defer reader.Close()

	// Set a reasonable timeout for reading
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	message, err := reader.ReadMessage(readCtx)
	require.NoError(t, err, "Should consume message from Kafka")
	t.Logf("Consumed message from topic %s: %d bytes", topicName, len(message.Value))

	return message.Value
}

// Validation functions for different message types

func validateTestMessage(t *testing.T, original, deserialized proto.Message) {
	origMsg := original.(*testpb.TestMessage)
	
	// The deserializer correctly returns *dynamicpb.Message, so we need to convert it
	deserMsg, err := convertDynamicToTestMessage(deserialized)
	require.NoError(t, err, "Should convert dynamic message to TestMessage")
	require.NotNil(t, deserMsg, "Converted message should not be nil")
	
	t.Logf("Original: %v", origMsg)
	t.Logf("Deserialized (after conversion): %v", deserMsg)
	
	// Validate all fields match
	assert.Equal(t, origMsg.GetId(), deserMsg.GetId(), "ID should match")
	assert.Equal(t, origMsg.GetName(), deserMsg.GetName(), "Name should match")
	assert.Equal(t, origMsg.GetAge(), deserMsg.GetAge(), "Age should match")
	assert.Equal(t, origMsg.GetEmail(), deserMsg.GetEmail(), "Email should match")
	assert.Equal(t, origMsg.GetTags(), deserMsg.GetTags(), "Tags should match")
	
	t.Logf("✅ TestMessage validation passed")
}

// convertDynamicToTestMessage converts a dynamic protobuf message to a concrete TestMessage
// This uses the serialize-deserialize pattern which is reliable and straightforward
func convertDynamicToTestMessage(dynamic proto.Message) (*testpb.TestMessage, error) {
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


// Infrastructure setup and helper functions

// requireKafkaRunning ensures Kafka is running and accessible
func requireKafkaRunning(t *testing.T) {
	conn, err := kafka.Dial("tcp", getKafkaBroker())
	require.NoError(t, err, "Kafka should be running at %s", getKafkaBroker())
	defer conn.Close()

	t.Logf("✅ Kafka is running at %s", getKafkaBroker())
}

// setupTestInfrastructure sets up the test topic and returns cleanup function
func setupTestInfrastructure(t *testing.T, ctx context.Context, topicName string) func() {
	// Create topic
	createKafkaTopic(t, ctx, topicName)

	// Return cleanup function
	return func() {
		deleteKafkaTopic(t, ctx, topicName)
	}
}

// createKafkaTopic creates a Kafka topic for testing
func createKafkaTopic(t *testing.T, ctx context.Context, topicName string) {
	conn, err := kafka.Dial("tcp", getKafkaBroker())
	require.NoError(t, err)
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Logf("Warning: Could not create topic %s (might already exist): %v", topicName, err)
	} else {
		t.Logf("Created Kafka topic: %s", topicName)
	}
}

// deleteKafkaTopic deletes a Kafka topic after testing
func deleteKafkaTopic(t *testing.T, ctx context.Context, topicName string) {
	conn, err := kafka.Dial("tcp", getKafkaBroker())
	if err != nil {
		t.Logf("Warning: Could not connect to Kafka for cleanup: %v", err)
		return
	}
	defer conn.Close()

	err = conn.DeleteTopics(topicName)
	if err != nil {
		t.Logf("Warning: Could not delete topic %s: %v", topicName, err)
	} else {
		t.Logf("Deleted Kafka topic: %s", topicName)
	}
}

// generateTestTopicName generates a unique topic name for testing
func generateTestTopicName() string {
	randomBytes := make([]byte, 4)
	rand.Read(randomBytes)
	return fmt.Sprintf("golang-integration-test-%x", randomBytes)
}

// getKafkaBroker returns the Kafka broker address
func getKafkaBroker() string {
	if broker := os.Getenv("KAFKA_BROKER"); broker != "" {
		return broker
	}
	return defaultKafkaBroker
}

// getAWSRegion returns the AWS region for GSR
func getAWSRegion() string {
	if region := os.Getenv("AWS_REGION"); region != "" {
		return region
	}
	if region := os.Getenv("AWS_DEFAULT_REGION"); region != "" {
		return region
	}
	return defaultAWSRegion
}
