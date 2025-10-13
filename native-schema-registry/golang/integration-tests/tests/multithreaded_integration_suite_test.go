package integration_tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// MultiThreadedIntegrationSuite tests concurrent GSR serialization/deserialization across multiple goroutines
// This is a STANDALONE test suite that does NOT inherit from BaseIntegrationSuite
type MultiThreadedIntegrationSuite struct {
	suite.Suite
	topicName string
	cleanup   func()
}

// TestResult represents the result of a goroutine's serialization/deserialization operations
type TestResult struct {
	GoroutineID      int
	OriginalMessage  *testpb.TestMessage
	SerializedData   []byte
	DeserializedData interface{}
	Error            error
	ProcessingTime   time.Duration
}

// SetupSuite initializes the test suite
func (s *MultiThreadedIntegrationSuite) SetupSuite() {
	s.T().Log("=== Setting up MultiThreaded Integration Suite ===")
	
	// Verify Kafka is running
	s.requireKafkaRunning()
	
	s.T().Log("=== MultiThreaded Integration Suite Setup Complete ===")
}

// SetupTest is called before each test method
func (s *MultiThreadedIntegrationSuite) SetupTest() {
	s.T().Log("Setting up MultiThreaded Test")
	
	// Generate unique topic name for this test
	s.topicName = s.generateTestTopicName()
	s.T().Logf("Test topicName: %s", s.topicName)
	
	// Set up test infrastructure
	s.cleanup = s.setupTestInfrastructure()
	
	s.T().Log("MultiThreaded test setup complete")
}

// TearDownTest is called after each test method
func (s *MultiThreadedIntegrationSuite) TearDownTest() {
	s.T().Log("Starting MultiThreaded test teardown...")
	
	if s.cleanup != nil {
		s.cleanup()
		s.cleanup = nil
	}
	
	s.T().Log("MultiThreaded test teardown complete")
}

// TestConcurrentSerializationDeserialization tests GSR serializer/deserializer thread safety
// across 2 concurrent goroutines as requested by the user
func (s *MultiThreadedIntegrationSuite) TestConcurrentSerializationDeserialization() {
	if s.shouldSkipIntegrationTests() {
		s.T().Skip("Skipping integration test")
	}
	
	s.T().Log("--- Starting Concurrent Serialization/Deserialization Test ---")
	
	const numGoroutines = 2 
	
	// Create GSR configuration
	gsrConfigAbsolutePath, err := filepath.Abs("./gsr.properties")
	require.NoError(s.T(), err, "Should get absolute path of gsr.properties")
	
	// Create test messages for each goroutine
	testMessages := s.createTestMessages(numGoroutines)
	
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: testMessages[0].ProtoReflect().Descriptor(),
		common.GSRConfigPathKey:             gsrConfigAbsolutePath,
	}
	config := common.NewConfiguration(configMap)
	
	// Create shared serializer and deserializer instances (thread-safe)
	gsr_serializer, err := serializer.NewSerializer(config)
	require.NoError(s.T(), err, "Should create serializer")
	
	gsr_deserializer, err := deserializer.NewDeserializer(config)
	require.NoError(s.T(), err, "Should create deserializer")
	
	// Channel to collect results from goroutines
	resultsChan := make(chan TestResult, numGoroutines)
	
	// WaitGroup to synchronize goroutines
	var wg sync.WaitGroup
	
	s.T().Logf("Starting %d concurrent goroutines for serialization/deserialization", numGoroutines)
	
	// Launch concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go s.concurrentSerializeDeserialize(i, testMessages[i], gsr_serializer, gsr_deserializer, resultsChan, &wg)
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	close(resultsChan)
	
	// Collect and validate results
	s.validateConcurrentResults(resultsChan, numGoroutines)
	
	s.T().Log("--- Concurrent Serialization/Deserialization Test Complete ---")
}

// concurrentSerializeDeserialize performs serialization and deserialization in a goroutine
func (s *MultiThreadedIntegrationSuite) concurrentSerializeDeserialize(
	goroutineID int,
	message *testpb.TestMessage,
	gsr_serializer *serializer.Serializer,
	gsr_deserializer *deserializer.Deserializer,
	resultsChan chan<- TestResult,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	
	result := TestResult{
		GoroutineID:     goroutineID,
		OriginalMessage: message,
	}
	
	startTime := time.Now()
	s.T().Logf("Goroutine %d: Starting serialization/deserialization", goroutineID)
	
	// Step 1: Serialize the message
	serializedData, err := gsr_serializer.Serialize(s.topicName, message)
	if err != nil {
		result.Error = fmt.Errorf("goroutine %d serialization failed: %w", goroutineID, err)
		result.ProcessingTime = time.Since(startTime)
		resultsChan <- result
		return
	}
	result.SerializedData = serializedData
	
	s.T().Logf("Goroutine %d: Serialized message (%d bytes)", goroutineID, len(serializedData))
	
	// Step 2: Publish to Kafka (simulating real-world usage)
	ctx := context.Background()
	s.publishMessageToKafka(ctx, fmt.Sprintf("%s-goroutine-%d", s.topicName, goroutineID), serializedData)
	
	// Step 3: Consume from Kafka
	consumedData := s.consumeMessageFromKafka(ctx, fmt.Sprintf("%s-goroutine-%d", s.topicName, goroutineID))
	
	// Step 4: Verify the data can be deserialized
	canDeserialize, err := gsr_deserializer.CanDeserialize(consumedData)
	if err != nil {
		result.Error = fmt.Errorf("goroutine %d CanDeserialize check failed: %w", goroutineID, err)
		result.ProcessingTime = time.Since(startTime)
		resultsChan <- result
		return
	}
	
	if !canDeserialize {
		result.Error = fmt.Errorf("goroutine %d data cannot be deserialized", goroutineID)
		result.ProcessingTime = time.Since(startTime)
		resultsChan <- result
		return
	}
	
	// Step 5: Deserialize the message
	deserializedMessage, err := gsr_deserializer.Deserialize(s.topicName, consumedData)
	if err != nil {
		result.Error = fmt.Errorf("goroutine %d deserialization failed: %w", goroutineID, err)
		result.ProcessingTime = time.Since(startTime)
		resultsChan <- result
		return
	}
	result.DeserializedData = deserializedMessage
	result.ProcessingTime = time.Since(startTime)
	
	s.T().Logf("Goroutine %d: Completed in %v", goroutineID, result.ProcessingTime)
	
	resultsChan <- result
}

// validateConcurrentResults validates the results from all goroutines
func (s *MultiThreadedIntegrationSuite) validateConcurrentResults(resultsChan <-chan TestResult, expectedCount int) {
	results := make([]TestResult, 0, expectedCount)
	
	// Collect all results
	for result := range resultsChan {
		results = append(results, result)
	}
	
	// Verify we got results from all goroutines
	require.Equal(s.T(), expectedCount, len(results), "Should receive results from all goroutines")
	
	// Validate each result
	for _, result := range results {
		s.T().Logf("Validating result from goroutine %d (processed in %v)", 
			result.GoroutineID, result.ProcessingTime)
		
		// Check for errors
		require.NoError(s.T(), result.Error, "Goroutine %d should not have errors", result.GoroutineID)
		
		// Validate serialization produced data
		require.NotEmpty(s.T(), result.SerializedData, "Goroutine %d should have serialized data", result.GoroutineID)
		
		// Validate deserialization produced data
		require.NotNil(s.T(), result.DeserializedData, "Goroutine %d should have deserialized data", result.GoroutineID)
		
		// Convert deserialized proto.Message back to TestMessage for comparison
		protoMessage, ok := result.DeserializedData.(proto.Message)
		require.True(s.T(), ok, "Goroutine %d: DeserializedData should be a proto.Message, got %T", result.GoroutineID, result.DeserializedData)
		
		deserializedTestMessage, err := s.convertDynamicToTestMessage(protoMessage)
		require.NoError(s.T(), err, "Goroutine %d should convert dynamic message", result.GoroutineID)
		
		// Validate data integrity - all fields should match
		assert.Equal(s.T(), result.OriginalMessage.GetId(), deserializedTestMessage.GetId(), 
			"Goroutine %d: ID should match", result.GoroutineID)
		assert.Equal(s.T(), result.OriginalMessage.GetName(), deserializedTestMessage.GetName(), 
			"Goroutine %d: Name should match", result.GoroutineID)
		assert.Equal(s.T(), result.OriginalMessage.GetAge(), deserializedTestMessage.GetAge(), 
			"Goroutine %d: Age should match", result.GoroutineID)
		assert.Equal(s.T(), result.OriginalMessage.GetEmail(), deserializedTestMessage.GetEmail(), 
			"Goroutine %d: Email should match", result.GoroutineID)
		assert.Equal(s.T(), result.OriginalMessage.GetTags(), deserializedTestMessage.GetTags(), 
			"Goroutine %d: Tags should match", result.GoroutineID)
		
		s.T().Logf("✅ Goroutine %d validation passed", result.GoroutineID)
	}
	
	// Calculate and log performance metrics
	s.logPerformanceMetrics(results)
	
	s.T().Logf("✅ All %d goroutines completed successfully with data integrity preserved", expectedCount)
}

// logPerformanceMetrics logs performance statistics from concurrent operations
func (s *MultiThreadedIntegrationSuite) logPerformanceMetrics(results []TestResult) {
	if len(results) == 0 {
		return
	}
	
	var totalTime time.Duration
	minTime := results[0].ProcessingTime
	maxTime := results[0].ProcessingTime
	
	for _, result := range results {
		totalTime += result.ProcessingTime
		if result.ProcessingTime < minTime {
			minTime = result.ProcessingTime
		}
		if result.ProcessingTime > maxTime {
			maxTime = result.ProcessingTime
		}
	}
	
	avgTime := totalTime / time.Duration(len(results))
	
	s.T().Logf("📊 Performance Metrics:")
	s.T().Logf("  - Goroutines: %d", len(results))
	s.T().Logf("  - Average time: %v", avgTime)
	s.T().Logf("  - Min time: %v", minTime)
	s.T().Logf("  - Max time: %v", maxTime)
	s.T().Logf("  - Total time: %v", totalTime)
}

// createTestMessages creates unique test messages for each goroutine
func (s *MultiThreadedIntegrationSuite) createTestMessages(count int) []*testpb.TestMessage {
	messages := make([]*testpb.TestMessage, count)
	
	for i := 0; i < count; i++ {
		messages[i] = &testpb.TestMessage{
			Id:    fmt.Sprintf("multithreaded-test-goroutine-%d-%d", i, time.Now().UnixNano()),
			Name:  fmt.Sprintf("MultiThreaded Test User %d", i),
			Age:   int32(25 + i), // Different ages for each goroutine
			Email: fmt.Sprintf("multithreaded.goroutine.%d@example.com", i),
			Tags:  []string{"multithreaded", "concurrent", fmt.Sprintf("goroutine-%d", i), "gsr", "protobuf"},
		}
	}
	
	return messages
}

// convertDynamicToTestMessage converts a dynamic protobuf message to a concrete TestMessage
func (s *MultiThreadedIntegrationSuite) convertDynamicToTestMessage(dynamic proto.Message) (*testpb.TestMessage, error) {
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

// Infrastructure setup and helper functions (standalone implementations)

// requireKafkaRunning ensures Kafka is running and accessible
func (s *MultiThreadedIntegrationSuite) requireKafkaRunning() {
	conn, err := kafka.Dial("tcp", s.getKafkaBroker())
	require.NoError(s.T(), err, "Kafka should be running at %s", s.getKafkaBroker())
	defer conn.Close()
	
	s.T().Logf("✅ Kafka is running at %s", s.getKafkaBroker())
}

// setupTestInfrastructure sets up the test topic and returns cleanup function
func (s *MultiThreadedIntegrationSuite) setupTestInfrastructure() func() {
	ctx := context.Background()
	
	// Create main topic
	s.createKafkaTopic(ctx, s.topicName)
	
	// Create topics for each goroutine (2 goroutines)
	for i := 0; i < 2; i++ {
		goroutineTopicName := fmt.Sprintf("%s-goroutine-%d", s.topicName, i)
		s.createKafkaTopic(ctx, goroutineTopicName)
	}
	
	return func() {
		// Cleanup main topic
		s.deleteKafkaTopic(ctx, s.topicName)
		
		// Cleanup goroutine topics
		for i := 0; i < 2; i++ {
			goroutineTopicName := fmt.Sprintf("%s-goroutine-%d", s.topicName, i)
			s.deleteKafkaTopic(ctx, goroutineTopicName)
		}
	}
}

// publishMessageToKafka publishes data to a Kafka topic
func (s *MultiThreadedIntegrationSuite) publishMessageToKafka(ctx context.Context, topicName string, data []byte) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(s.getKafkaBroker()),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()
	
	message := kafka.Message{
		Key:   []byte(fmt.Sprintf("multithreaded-test-key-%d", time.Now().UnixNano())),
		Value: data,
	}
	
	err := writer.WriteMessages(ctx, message)
	for i := 1; i < 5 && err != nil; i++ {
		err = writer.WriteMessages(ctx, message)
		time.Sleep(time.Duration(i) * time.Second)
	}
	require.NoError(s.T(), err, "Should publish message to Kafka")
}

// consumeMessageFromKafka consumes a single message from a Kafka topic
func (s *MultiThreadedIntegrationSuite) consumeMessageFromKafka(ctx context.Context, topicName string) []byte {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{s.getKafkaBroker()},
		Topic:   topicName,
		GroupID: fmt.Sprintf("multithreaded-test-group-%d", time.Now().UnixNano()),
	})
	defer reader.Close()
	
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	
	message, err := reader.ReadMessage(readCtx)
	require.NoError(s.T(), err, "Should consume message from Kafka")
	
	return message.Value
}

// createKafkaTopic creates a Kafka topic for testing
func (s *MultiThreadedIntegrationSuite) createKafkaTopic(ctx context.Context, topicName string) {
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
	
	// Wait for topic to be ready
	s.waitForTopicReady(ctx, topicName)
}

// deleteKafkaTopic deletes a Kafka topic after testing with timeout handling
func (s *MultiThreadedIntegrationSuite) deleteKafkaTopic(ctx context.Context, topicName string) {
	// Create a timeout context for the delete operation
	deleteCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	
	done := make(chan error, 1)
	
	go func() {
		conn, err := kafka.Dial("tcp", s.getKafkaBroker())
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		
		err = conn.DeleteTopics(topicName)
		done <- err
	}()
	
	select {
	case err := <-done:
		if err != nil {
			s.T().Logf("Warning: Could not delete topic %s: %v", topicName, err)
		} else {
			s.T().Logf("Deleted Kafka topic: %s", topicName)
		}
	case <-deleteCtx.Done():
		s.T().Logf("Warning: Timeout deleting topic %s, continuing with cleanup", topicName)
	}
	
	// Add a small delay between topic deletions to avoid overwhelming Kafka
	time.Sleep(100 * time.Millisecond)
}

// waitForTopicReady waits until a Kafka topic is ready and available for operations
func (s *MultiThreadedIntegrationSuite) waitForTopicReady(ctx context.Context, topicName string) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	// Check immediately first
	conn, err := kafka.Dial("tcp", s.getKafkaBroker())
	if err == nil {
		partitions, err := conn.ReadPartitions(topicName)
		conn.Close()
		
		if err == nil && len(partitions) > 0 {
			s.T().Logf("✅ Topic %s is ready with %d partition(s)", topicName, len(partitions))
			return
		}
	}
	
	// Then poll every 5 seconds
	for {
		select {
		case <-timeoutCtx.Done():
			require.Fail(s.T(), "Timeout waiting for topic to be ready",
				"Topic %s was not ready after 30 seconds", topicName)
			return
		case <-ticker.C:
			conn, err := kafka.Dial("tcp", s.getKafkaBroker())
			if err != nil {
				continue
			}
			
			partitions, err := conn.ReadPartitions(topicName)
			conn.Close()
			
			if err == nil && len(partitions) > 0 {
				s.T().Logf("✅ Topic %s is ready with %d partition(s)", topicName, len(partitions))
				return
			}
		}
	}
}

// generateTestTopicName generates a unique topic name for testing
func (s *MultiThreadedIntegrationSuite) generateTestTopicName() string {
	randomBytes := make([]byte, 4)
	rand.Read(randomBytes)
	return fmt.Sprintf("multithreaded-gsr-integration-test-%x", randomBytes)
}

// getKafkaBroker returns the Kafka broker address
func (s *MultiThreadedIntegrationSuite) getKafkaBroker() string {
	if broker := os.Getenv("KAFKA_BROKER"); broker != "" {
		return broker
	}
	return defaultKafkaBroker
}

// shouldSkipIntegrationTests checks if integration tests should be skipped
func (s *MultiThreadedIntegrationSuite) shouldSkipIntegrationTests() bool {
	return os.Getenv("SKIP_INTEGRATION_TESTS") == "true"
}

// TestMultiThreadedIntegrationSuite runs the MultiThreaded integration test suite
func TestMultiThreadedIntegrationSuite(t *testing.T) {
	suite.Run(t, new(MultiThreadedIntegrationSuite))
}
