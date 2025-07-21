package integration_tests

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// AvroIntegrationSuite tests AVRO integration with Kafka and AWS GSR
type AvroIntegrationSuite struct {
	BaseIntegrationSuite
}

// TestAvroKafkaIntegration tests the complete end-to-end flow for AVRO
func (s *AvroIntegrationSuite) TestAvroKafkaIntegration() {
	if s.shouldSkipIntegrationTests() {
		s.T().Skip("Skipping integration test")
	}
	
	s.T().Log("--- Starting AVRO Kafka Integration Test ---")
	
	// Create AVRO test data
	message := s.createAvroTestData()
	
	// Run the integration test with validation
	s.runKafkaIntegrationTest(message, s.validateAvroMessage)
	
	s.T().Log("--- AVRO Kafka Integration Test Complete ---")
}

// createAvroTestData creates AVRO-compatible test data as map[string]interface{}
func (s *AvroIntegrationSuite) createAvroTestData() map[string]interface{} {
	return map[string]interface{}{
		"id":       "avro-suite-test-123",
		"name":     "AVRO Suite Integration Test",
		"age":      int32(42),
		"email":    "avro.suite@example.com",
		"isActive": true,
		"tags":     []interface{}{"integration", "test", "avro", "suite"},
		"metadata": map[string]interface{}{
			"version":    "1.0",
			"framework":  "go-avro",
			"testType":   "integration",
		},
	}
}

// validateAvroMessage validates AVRO message round-trip
func (s *AvroIntegrationSuite) validateAvroMessage(original, deserialized interface{}) {
	origMap, ok := original.(map[string]interface{})
	require.True(s.T(), ok, "Original message should be map[string]interface{}, got %T", original)
	
	// For AVRO, the deserializer should return map[string]interface{} directly
	deserMap, ok := deserialized.(map[string]interface{})
	require.True(s.T(), ok, "Deserialized message should be map[string]interface{}, got %T", deserialized)
	
	s.T().Logf("Original AVRO: %v", origMap)
	s.T().Logf("Deserialized AVRO: %v", deserMap)
	
	// Validate all fields match
	assert.Equal(s.T(), origMap["id"], deserMap["id"], "ID should match")
	assert.Equal(s.T(), origMap["name"], deserMap["name"], "Name should match")
	assert.Equal(s.T(), origMap["age"], deserMap["age"], "Age should match")
	assert.Equal(s.T(), origMap["email"], deserMap["email"], "Email should match")
	assert.Equal(s.T(), origMap["isActive"], deserMap["isActive"], "IsActive should match")
	assert.Equal(s.T(), origMap["tags"], deserMap["tags"], "Tags should match")
	assert.Equal(s.T(), origMap["metadata"], deserMap["metadata"], "Metadata should match")
	
	s.T().Logf("✅ AVRO message validation passed")
}

// shouldSkipIntegrationTests checks if integration tests should be skipped
func (s *AvroIntegrationSuite) shouldSkipIntegrationTests() bool {
	return os.Getenv("SKIP_INTEGRATION_TESTS") == "true"
}

// TestAvroIntegrationSuite runs the AVRO integration test suite
func TestAvroIntegrationSuite(t *testing.T) {
	suite.Run(t, new(AvroIntegrationSuite))
}

