package integration_tests

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	gsrjson "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/json"
)

// TestJsonUser represents a test user for JSON integration tests
type TestJsonUser struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Age    int      `json:"age"`
	Email  string   `json:"email"`
	Active bool     `json:"active"`
	Tags   []string `json:"tags"`
}

// JsonIntegrationSuite tests JSON integration with Kafka and AWS GSR
type JsonIntegrationSuite struct {
	BaseIntegrationSuite
}

// TestJsonKafkaIntegration tests the complete end-to-end flow for JSON Schema
func (s *JsonIntegrationSuite) TestJsonKafkaIntegration() {
	if s.shouldSkipIntegrationTests() {
		s.T().Skip("Skipping integration test")
	}

	s.T().Log("--- Starting JSON Schema Kafka Integration Test ---")

	// Create test user struct
	user := &TestJsonUser{
		ID:     "json-suite-test-789",
		Name:   "JSON Suite Integration Test User",
		Age:    28,
		Email:  "json.suite@example.com",
		Active: true,
		Tags:   []string{"integration", "test", "json", "suite"},
	}

	// Define corresponding JSON schema
	jsonSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"title": "TestJsonUser",
		"properties": {
			"id": {
				"type": "string",
				"description": "The unique identifier for the user"
			},
			"name": {
				"type": "string",
				"description": "The user's full name"
			},
			"age": {
				"type": "integer",
				"minimum": 0,
				"maximum": 150,
				"description": "The user's age"
			},
			"email": {
				"type": "string",
				"format": "email",
				"description": "The user's email address"
			},
			"active": {
				"type": "boolean",
				"description": "Whether the user is active"
			},
			"tags": {
				"type": "array",
				"items": {
					"type": "string"
				},
				"description": "List of tags associated with the user"
			}
		},
		"required": ["id", "name", "age", "email", "active"],
		"additionalProperties": false
	}`

	// Marshal user to JSON string
	userJSON, err := json.Marshal(user)
	require.NoError(s.T(), err, "Should marshal user to JSON")

	// Create JsonDataWithSchema with schema and data
	jsonDataWithSchema := &gsrjson.JsonDataWithSchema{
		Schema:  jsonSchema,
		Payload: string(userJSON),
	}

	// Create JSON configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatJSON,
	}
	config := common.NewConfiguration(configMap)

	// Run the integration test with validation and configuration
	s.runKafkaIntegrationTest(jsonDataWithSchema, s.validateJsonMessage, config)

	s.T().Log("--- JSON Schema Kafka Integration Test Complete ---")
}

// validateJsonMessage validates JSON message round-trip
func (s *JsonIntegrationSuite) validateJsonMessage(original, deserialized interface{}) {
	// Original should be *gsrjson.JsonDataWithSchema
	origDataWithSchema := original.(*gsrjson.JsonDataWithSchema)

	// Parse the original JSON payload to get the original data
	var origUser TestJsonUser
	err := json.Unmarshal([]byte(origDataWithSchema.Payload), &origUser)
	require.NoError(s.T(), err, "Should unmarshal original payload")

	// Deserialized should be a string (JSON payload)
	deserStr, ok := deserialized.(string)
	require.True(s.T(), ok, "Deserialized message should be a string, got %T", deserialized)

	// Parse the deserialized JSON string to get the actual data
	var deserMap map[string]interface{}
	err = json.Unmarshal([]byte(deserStr), &deserMap)
	require.NoError(s.T(), err, "Should unmarshal deserialized JSON string")

	s.T().Logf("Original: %+v", origUser)
	s.T().Logf("Deserialized: %+v", deserMap)

	// Validate all fields match
	assert.Equal(s.T(), origUser.ID, deserMap["id"], "ID should match")
	assert.Equal(s.T(), origUser.Name, deserMap["name"], "Name should match")

	// JSON numbers are deserialized as float64, so we need to convert
	assert.Equal(s.T(), float64(origUser.Age), deserMap["age"], "Age should match")
	assert.Equal(s.T(), origUser.Email, deserMap["email"], "Email should match")
	assert.Equal(s.T(), origUser.Active, deserMap["active"], "Active should match")

	// Tags array validation
	deserTags, ok := deserMap["tags"].([]interface{})
	require.True(s.T(), ok, "Tags should be []interface{}")
	require.Equal(s.T(), len(origUser.Tags), len(deserTags), "Tags length should match")

	for i, tag := range origUser.Tags {
		assert.Equal(s.T(), tag, deserTags[i], "Tag %d should match", i)
	}

	s.T().Logf("✅ JSON Schema message validation passed")
}

// TestJsonKafkaIntegrationWithComplexData tests JSON integration with more complex nested data
func (s *JsonIntegrationSuite) TestJsonKafkaIntegrationWithComplexData() {
	if s.shouldSkipIntegrationTests() {
		s.T().Skip("Skipping integration test")
	}

	s.T().Log("--- Starting JSON Schema Complex Data Integration Test ---")

	// Create more complex test data
	complexData := map[string]interface{}{
		"user_profile": map[string]interface{}{
			"id":     "complex-test-123",
			"name":   "Complex Test User",
			"age":    30,
			"email":  "complex@example.com",
			"active": true,
			"metadata": map[string]interface{}{
				"created_at": "2023-01-01T00:00:00Z",
				"last_login": "2023-12-31T23:59:59Z",
				"preferences": map[string]interface{}{
					"theme":    "dark",
					"language": "en",
					"timezone": "UTC",
				},
			},
		},
		"permissions": []interface{}{
			"read", "write", "admin",
		},
		"statistics": map[string]interface{}{
			"login_count":    42,
			"posts_created":  156,
			"average_rating": 4.7,
		},
	}

	// Define corresponding complex JSON schema
	complexSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"title": "ComplexUserData",
		"properties": {
			"user_profile": {
				"type": "object",
				"properties": {
					"id": {"type": "string"},
					"name": {"type": "string"},
					"age": {"type": "integer"},
					"email": {"type": "string"},
					"active": {"type": "boolean"},
					"metadata": {
						"type": "object",
						"properties": {
							"created_at": {"type": "string"},
							"last_login": {"type": "string"},
							"preferences": {
								"type": "object",
								"properties": {
									"theme": {"type": "string"},
									"language": {"type": "string"},
									"timezone": {"type": "string"}
								}
							}
						}
					}
				},
				"required": ["id", "name", "age", "email", "active"]
			},
			"permissions": {
				"type": "array",
				"items": {"type": "string"}
			},
			"statistics": {
				"type": "object",
				"properties": {
					"login_count": {"type": "integer"},
					"posts_created": {"type": "integer"},
					"average_rating": {"type": "number"}
				}
			}
		},
		"required": ["user_profile", "permissions", "statistics"]
	}`

	// Marshal complex data to JSON string
	complexDataJSON, err := json.Marshal(complexData)
	require.NoError(s.T(), err, "Should marshal complex data to JSON")

	// Create JsonDataWithSchema with complex schema and data
	jsonDataWithSchema := &gsrjson.JsonDataWithSchema{
		Schema:  complexSchema,
		Payload: string(complexDataJSON),
	}

	// Create JSON configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatJSON,
	}
	config := common.NewConfiguration(configMap)

	// Run the integration test with validation and configuration
	s.runKafkaIntegrationTest(jsonDataWithSchema, s.validateComplexJsonMessage, config)

	s.T().Log("--- JSON Schema Complex Data Integration Test Complete ---")
}

// validateComplexJsonMessage validates complex JSON message round-trip
func (s *JsonIntegrationSuite) validateComplexJsonMessage(original, deserialized interface{}) {
	// Original should be *gsrjson.JsonDataWithSchema
	origDataWithSchema := original.(*gsrjson.JsonDataWithSchema)

	// Parse the original JSON payload to get the original data
	var origData map[string]interface{}
	err := json.Unmarshal([]byte(origDataWithSchema.Payload), &origData)
	require.NoError(s.T(), err, "Should unmarshal original complex payload")

	// Deserialized should be a string (JSON payload)
	deserStr, ok := deserialized.(string)
	require.True(s.T(), ok, "Deserialized message should be a string, got %T", deserialized)

	// Parse the deserialized JSON string to get the actual data
	var deserMap map[string]interface{}
	err = json.Unmarshal([]byte(deserStr), &deserMap)
	require.NoError(s.T(), err, "Should unmarshal deserialized JSON string")

	s.T().Logf("Original: %+v", origData)
	s.T().Logf("Deserialized: %+v", deserMap)

	// Validate user_profile
	origProfile := origData["user_profile"].(map[string]interface{})
	deserProfile, ok := deserMap["user_profile"].(map[string]interface{})
	require.True(s.T(), ok, "user_profile should be a map")

	assert.Equal(s.T(), origProfile["id"], deserProfile["id"], "Profile ID should match")
	assert.Equal(s.T(), origProfile["name"], deserProfile["name"], "Profile name should match")
	assert.Equal(s.T(), origProfile["age"], deserProfile["age"], "Profile age should match")
	assert.Equal(s.T(), origProfile["email"], deserProfile["email"], "Profile email should match")
	assert.Equal(s.T(), origProfile["active"], deserProfile["active"], "Profile active should match")

	// Validate permissions
	origPermissions := origData["permissions"].([]interface{})
	deserPermissions, ok := deserMap["permissions"].([]interface{})
	require.True(s.T(), ok, "permissions should be an array")
	require.Equal(s.T(), len(origPermissions), len(deserPermissions), "Permissions length should match")

	for i, perm := range origPermissions {
		assert.Equal(s.T(), perm, deserPermissions[i], "Permission %d should match", i)
	}

	// Validate statistics
	origStats := origData["statistics"].(map[string]interface{})
	deserStats, ok := deserMap["statistics"].(map[string]interface{})
	require.True(s.T(), ok, "statistics should be a map")

	assert.Equal(s.T(), origStats["login_count"], deserStats["login_count"], "Login count should match")
	assert.Equal(s.T(), origStats["posts_created"], deserStats["posts_created"], "Posts created should match")
	assert.Equal(s.T(), origStats["average_rating"], deserStats["average_rating"], "Average rating should match")

	s.T().Logf("✅ Complex JSON Schema message validation passed")
}

// shouldSkipIntegrationTests checks if integration tests should be skipped
func (s *JsonIntegrationSuite) shouldSkipIntegrationTests() bool {
	return os.Getenv("SKIP_INTEGRATION_TESTS") == "true"
}

// TestJsonIntegrationSuite runs the JSON integration test suite
func TestJsonIntegrationSuite(t *testing.T) {
	suite.Run(t, new(JsonIntegrationSuite))
}
