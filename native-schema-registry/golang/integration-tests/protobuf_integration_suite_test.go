package integration_tests

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// ProtobufIntegrationSuite tests Protobuf integration with Kafka and AWS GSR
type ProtobufIntegrationSuite struct {
	BaseIntegrationSuite
}

// TestProtobufKafkaIntegration tests the complete end-to-end flow for Protobuf
func (s *ProtobufIntegrationSuite) TestProtobufKafkaIntegration() {
	if s.shouldSkipIntegrationTests() {
		s.T().Skip("Skipping integration test")
	}

	s.T().Log("--- Starting Protobuf Kafka Integration Test ---")

	// Create protobuf test message
	message := &testpb.TestMessage{
		Id:    "protobuf-suite-test-123",
		Name:  "Protobuf Suite Integration Test",
		Age:   42,
		Email: "protobuf.suite@example.com",
		Tags:  []string{"integration", "test", "protobuf", "suite"},
	}

	// Extract the message descriptor using protobuf reflection
	messageDescriptor := message.ProtoReflect().Descriptor()

	// Create Protobuf configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
	}
	config := common.NewConfiguration(configMap)

	// Run the integration test with validation and configuration
	s.runKafkaIntegrationTest(message, s.validateProtobufMessage, config)

	s.T().Log("--- Protobuf Kafka Integration Test Complete ---")
}

// validateProtobufMessage validates protobuf message round-trip
func (s *ProtobufIntegrationSuite) validateProtobufMessage(original, deserialized interface{}) {
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

	s.T().Logf("✅ Protobuf message validation passed")
}

// convertDynamicToTestMessage converts a dynamic protobuf message to a concrete TestMessage
func (s *ProtobufIntegrationSuite) convertDynamicToTestMessage(dynamic proto.Message) (*testpb.TestMessage, error) {
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
func (s *ProtobufIntegrationSuite) shouldSkipIntegrationTests() bool {
	return os.Getenv("SKIP_INTEGRATION_TESTS") == "true"
}

// TestProtobufIntegrationSuite runs the Protobuf integration test suite
func TestProtobufIntegrationSuite(t *testing.T) {
	suite.Run(t, new(ProtobufIntegrationSuite))
}
