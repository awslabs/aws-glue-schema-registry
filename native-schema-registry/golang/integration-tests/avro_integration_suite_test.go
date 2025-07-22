package integration_tests

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// TestUser represents a test user for AVRO integration tests
type TestUser struct {
	ID    string   `avro:"id"`
	Name  string   `avro:"name"`
	Age   int      `avro:"age"`
	Email string   `avro:"email"`
	Tags  []string `avro:"tags"`
}

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

	// Create test user struct
	user := &TestUser{
		ID:    "avro-suite-test-456",
		Name:  "AVRO Suite Integration Test User",
		Age:   35,
		Email: "avro.suite@example.com",
		Tags:  []string{"integration", "test", "avro", "suite"},
	}

	// Define corresponding AVRO schema
	avroSchema := `{
		"type": "record",
		"name": "TestUser",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"},
			{"name": "email", "type": "string"},
			{"name": "tags", "type": {"type": "array", "items": "string"}}
		]
	}`

	// Create AvroRecord with schema and data
	avroRecord := &avro.AvroRecord{
		Schema: avroSchema,
		Data:   user,
	}

	// Create AVRO configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
	}
	config := common.NewConfiguration(configMap)

	// Run the integration test with validation and configuration
	s.runKafkaIntegrationTest(avroRecord, s.validateAvroMessage, config)

	s.T().Log("--- AVRO Kafka Integration Test Complete ---")
}

// validateAvroMessage validates AVRO message round-trip
func (s *AvroIntegrationSuite) validateAvroMessage(original, deserialized interface{}) {
	// Original should be *avro.AvroRecord
	origRecord := original.(*avro.AvroRecord)
	origUser := origRecord.Data.(*TestUser)

	// Deserialized should be a map[string]interface{} (from hamba/avro)
	deserMap, ok := deserialized.(map[string]interface{})
	require.True(s.T(), ok, "Deserialized message should be a map[string]interface{}, got %T", deserialized)

	s.T().Logf("Original: %+v", origUser)
	s.T().Logf("Deserialized: %+v", deserMap)

	// Validate all fields match
	assert.Equal(s.T(), origUser.ID, deserMap["id"], "ID should match")
	assert.Equal(s.T(), origUser.Name, deserMap["name"], "Name should match")
	assert.Equal(s.T(), origUser.Age, deserMap["age"], "Age should match") // hamba/avro returns int64
	assert.Equal(s.T(), origUser.Email, deserMap["email"], "Email should match")
	
	// Tags array validation
	deserTags, ok := deserMap["tags"].([]interface{})
	require.True(s.T(), ok, "Tags should be []interface{}")
	require.Equal(s.T(), len(origUser.Tags), len(deserTags), "Tags length should match")
	
	for i, tag := range origUser.Tags {
		assert.Equal(s.T(), tag, deserTags[i], "Tag %d should match", i)
	}

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
