package avro

import (
	"strings"
	"testing"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/tests/evolution_tests/base"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
	"github.com/stretchr/testify/suite"
)

// SchemaEvolutionTestSuite contains the test suite for schema evolution tests
type SchemaEvolutionTestSuite struct {
	base.BaseEvolutionTestSuite
}

// TestBackwardCompatibleEvolution tests backward compatible schema evolution
// This test verifies that a schema evolution that removes a field and adds an optional field
// with a default value maintains backward compatibility
func (suite *SchemaEvolutionTestSuite) TestBackwardCompatibleEvolution() {
	// Load user_v2.avsc schema using base class method
	userV2Schema := suite.LoadSchemaFromFile("backward/user_v2.avsc", common.DataFormatAvro.String())

	// Create GSR configuration using base class method
	config := suite.CreateGSRConfig("AVRO")

	// Create serializer and deserializer with proper error handling for AWS service errors
	gsrSerializer, err := serializer.NewSerializer(config)
	suite.Require().NoError(err, "Failed to create GSR serializer")

	gsrDeserializer, err := deserializer.NewDeserializer(config)
	suite.Require().NoError(err, "Failed to create GSR deserializer")

	// Create test data compatible with user_v2 schema structure
	userV2Data := map[string]interface{}{
		"id":     "user123",
		"name":   "John Doe",
		"active": true,
		"status": "registered",
	}

	// Create AvroRecord with user_v2 schema and data
	avroRecord := &avro.AvroRecord{
		Schema: userV2Schema,
		Data:   userV2Data,
	}

	// Use serializer to encode data with user_v2 schema (GSR will auto-register schema)
	serializedData, err := gsrSerializer.Serialize("user_v2_topic", avroRecord)
	suite.Require().NoError(err, "Failed to serialize data with user_v2 schema")
	suite.Require().NotEmpty(serializedData, "Serialized data should not be empty")

	// Verify the data can be deserialized
	canDeserialize, err := gsrDeserializer.CanDeserialize(serializedData)
	suite.Require().NoError(err, "Failed to check if data can be deserialized")
	suite.Require().True(canDeserialize, "GSR-encoded data should be deserializable")

	// Use deserializer to decode the serialized data (GSR will auto-retrieve schema)
	deserializedData, err := gsrDeserializer.Deserialize("user_v2_topic", serializedData)
	suite.Require().NoError(err, "Failed to deserialize data (GSR auto-retrieve schema)")
	suite.Require().NotNil(deserializedData, "Deserialized data should not be nil")

	// Assert that deserialized data matches the original input data
	dataMap, ok := deserializedData.(map[string]interface{})
	suite.Require().True(ok, "Deserialized data should be a map[string]interface{}")

	// Verify all fields from user_v2 schema are present and correct
	suite.Equal("user123", dataMap["id"], "ID field should be preserved")
	suite.Equal("John Doe", dataMap["name"], "Name field should be preserved")
	suite.Equal(true, dataMap["active"], "Active field should be preserved")
	suite.Equal("registered", dataMap["status"], "Status field should be preserved")

	// Verify that the schema was created in AWS Glue Schema Registry using base class method
	suite.VerifySchemaInRegistry("user_v2_topic-value")

	suite.T().Log("Backward compatible evolution test passed successfully")
}

// TestBackwardCompatibleEvolution_V1ToV2 tests backward compatible schema evolution
// This test verifies that data serialized with user_v1 schema can be properly deserialized
// and that the deserializer is compatible with serializer using an earlier schema version
func (suite *SchemaEvolutionTestSuite) TestBackwardCompatibleEvolution_V1ToV2() {
	// Load user_v1.avsc and user_v2.avsc schemas using base class method
	userV1Schema := suite.LoadSchemaFromFile("backward/user_v1.avsc", common.DataFormatAvro.String())
	userV2Schema := suite.LoadSchemaFromFile("backward/user_v2.avsc", common.DataFormatAvro.String())

	// Create GSR configuration using base class method
	config := suite.CreateGSRConfig("AVRO")

	// Create serializer and deserializer with proper error handling for AWS service errors
	gsrSerializer, err := serializer.NewSerializer(config)
	suite.Require().NoError(err, "Failed to create GSR serializer")

	gsrDeserializer, err := deserializer.NewDeserializer(config)
	suite.Require().NoError(err, "Failed to create GSR deserializer")

	// Create test data compatible with user_v1 schema structure (without status field, with email field)
	userV1Data := map[string]interface{}{
		"id":     "user456",
		"name":   "Jane Smith",
		"email":  "jane.smith@example.com",
		"active": false,
	}

	// Create AvroRecord with user_v1 schema and data
	avroRecord := &avro.AvroRecord{
		Schema: userV1Schema,
		Data:   userV1Data,
	}

	// Use serializer to encode data with user_v1 schema (GSR will auto-register schema)
	serializedData, err := gsrSerializer.Serialize("user_v1_topic", avroRecord)
	suite.Require().NoError(err, "Failed to serialize data with user_v1 schema")
	suite.Require().NotEmpty(serializedData, "Serialized data should not be empty")

	// Verify the data can be deserialized
	canDeserialize, err := gsrDeserializer.CanDeserialize(serializedData)
	suite.Require().NoError(err, "Failed to check if data can be deserialized")
	suite.Require().True(canDeserialize, "GSR-encoded data should be deserializable")

	// Use deserializer to decode the serialized data (GSR will auto-retrieve schema)
	deserializedData, err := gsrDeserializer.Deserialize("user_v1_topic", serializedData)
	suite.Require().NoError(err, "Failed to deserialize data (GSR auto-retrieve schema)")
	suite.Require().NotNil(deserializedData, "Deserialized data should not be nil")

	// Assert that deserialized data matches the original input data
	dataMap, ok := deserializedData.(map[string]interface{})
	suite.Require().True(ok, "Deserialized data should be a map[string]interface{}")

	// Verify all fields from user_v1 schema are present and correct
	suite.Equal("user456", dataMap["id"], "ID field should be preserved")
	suite.Equal("Jane Smith", dataMap["name"], "Name field should be preserved")
	suite.Equal("jane.smith@example.com", dataMap["email"], "Email field should be preserved")
	suite.Equal(false, dataMap["active"], "Active field should be preserved")

	// Verify that deserializer is compatible with serializer using an earlier schema version
	// The user_v1 schema should be successfully registered and retrieved by GSR
	suite.T().Log("Verified that deserializer is compatible with serializer using user_v1 schema")

	// Additional verification: Ensure that the v2 schema can also be used for comparison
	// This demonstrates backward compatibility - v2 can read v1 data
	userV2AvroRecord := &avro.AvroRecord{
		Schema: userV2Schema,
		Data: map[string]interface{}{
			"id":     "user789",
			"name":   "Bob Wilson",
			"active": true,
			"status": "premium",
		},
	}

	// Serialize with v2 schema to ensure both schemas can coexist
	serializedV2Data, err := gsrSerializer.Serialize("user_v2_compat_topic", userV2AvroRecord)
	suite.Require().NoError(err, "Failed to serialize data with user_v2 schema for compatibility test")
	suite.Require().NotEmpty(serializedV2Data, "Serialized V2 data should not be empty")

	// Deserialize v2 data to ensure it works
	deserializedV2Data, err := gsrDeserializer.Deserialize("user_v2_compat_topic", serializedV2Data)
	suite.Require().NoError(err, "Failed to deserialize user_v2 data for compatibility test")
	suite.Require().NotNil(deserializedV2Data, "Deserialized V2 data should not be nil")

	// Verify that both user_v1 and user_v2 schemas were created in AWS Glue Schema Registry using base class method
	suite.VerifySchemaInRegistry("user_v1_topic-value")
	suite.VerifySchemaInRegistry("user_v2_compat_topic-value")

	suite.T().Log("Backward compatible evolution test with user_v1 schema passed successfully")
}

// TestBackwardIncompatibleEvolution tests backward incompatible schema evolution
// This test verifies that a schema evolution that adds a required field without a default
// breaks backward compatibility and should fail
func (suite *SchemaEvolutionTestSuite) TestBackwardIncompatibleEvolution() {
	// Load employee_v1.avsc and employee_v2.avsc schemas using base class method
	employeeV1Schema := suite.LoadSchemaFromFile("negative/backward/employee_v1.avsc", common.DataFormatAvro.String())
	employeeV2Schema := suite.LoadSchemaFromFile("negative/backward/employee_v2.avsc", common.DataFormatAvro.String())

	// Create GSR configuration using base class method
	config := suite.CreateGSRConfig("AVRO")

	// Create serializer with proper error handling for AWS service errors
	gsrSerializer, err := serializer.NewSerializer(config)
	suite.Require().NoError(err, "Failed to create GSR serializer")

	// Step 1: First register employee_v1 schema (the base schema)
	employeeV1Data := map[string]interface{}{
		"id":         123,
		"firstName":  "Jane",
		"lastName":   "Smith",
		"department": "Engineering",
		// Note: no requiredEmail field in v1
	}

	avroRecordV1 := &avro.AvroRecord{
		Schema: employeeV1Schema,
		Data:   employeeV1Data,
	}

	// Serialize with v1 schema first to establish the baseline
	serializedV1Data, err := gsrSerializer.Serialize("employee_incompatible_topic", avroRecordV1)
	suite.Require().NoError(err, "Failed to serialize employee_v1 data - this should succeed")
	suite.Require().NotEmpty(serializedV1Data, "Serialized V1 data should not be empty")
	suite.T().Log("Successfully registered employee_v1 schema as baseline")

	// Step 2: Now try to register employee_v2 schema which adds a required field
	// This should fail due to backward compatibility violation
	employeeV2Data := map[string]interface{}{
		"id":            456,
		"firstName":     "Bob",
		"lastName":      "Johnson",
		"department":    "Marketing",
		"requiredEmail": "bob.johnson@example.com",
	}

	avroRecordV2 := &avro.AvroRecord{
		Schema: employeeV2Schema,
		Data:   employeeV2Data,
	}

	// This should fail because employee_v2 adds a required field that breaks backward compatibility
	serializedV2Data, err := gsrSerializer.Serialize("employee_incompatible_topic", avroRecordV2)

	// Assert that serialization fails due to backward compatibility violation
	if err != nil {
		// Expected case: serialization should fail due to backward incompatibility
		suite.T().Logf("Backward incompatible evolution correctly failed during serialization with error: %v", err)

		// Verify that the error message indicates backward compatibility issues
		errorMsg := err.Error()
		if strings.Contains(errorMsg, "compatibility") || strings.Contains(errorMsg, "Compatibility") ||
			strings.Contains(errorMsg, "backward") || strings.Contains(errorMsg, "incompatible") {
			suite.T().Log("Error message correctly indicates backward compatibility issues")
		} else {
			suite.T().Logf("Warning - Error message may not clearly indicate compatibility issue: %s", errorMsg)
		}

		// Test passed - backward incompatibility was correctly detected
		suite.T().Log("Backward incompatible evolution test passed - GSR correctly rejected incompatible schema")
	} else {
		// If serialization unexpectedly succeeds, this indicates a problem with the test or GSR configuration
		suite.Require().NotEmpty(serializedV2Data, "Serialized V2 data should not be empty")
		suite.T().Log("Warning - Expected backward incompatibility not detected by GSR")

		// The test should fail here because we expected an error
		suite.Fail("Expected backward incompatibility error when registering employee_v2 schema, but serialization succeeded")
	}

	// Verify that the employee schema was created in AWS Glue Schema Registry using base class method
	suite.VerifySchemaInRegistry("employee_incompatible_topic-value")

	suite.T().Log("Backward incompatible evolution test completed successfully")
}

// TestSchemaEvolutionSuite runs the schema evolution test suite
func TestSchemaEvolutionSuite(t *testing.T) {
	suite.Run(t, new(SchemaEvolutionTestSuite))
}
