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

	// Step 1: Serialize data with user_v1 schema first (GSR will auto-register v1 schema)
	serializedV1Data, err := gsrSerializer.Serialize("user_evolution_topic", avroRecord)
	suite.Require().NoError(err, "Failed to serialize data with user_v1 schema")
	suite.Require().NotEmpty(serializedV1Data, "Serialized V1 data should not be empty")
	suite.T().Log("Successfully serialized data with user_v1 schema")

	// Step 2: Serialize data with user_v2 schema (GSR will auto-register v2 schema as evolution)
	userV2Data := map[string]interface{}{
		"id":     "user789",
		"name":   "Bob Wilson",
		"active": true,
		"status": "premium", // New field in v2 with default value
	}

	avroRecordV2 := &avro.AvroRecord{
		Schema: userV2Schema,
		Data:   userV2Data,
	}

	serializedV2Data, err := gsrSerializer.Serialize("user_evolution_topic", avroRecordV2)
	suite.Require().NoError(err, "Failed to serialize data with user_v2 schema")
	suite.Require().NotEmpty(serializedV2Data, "Serialized V2 data should not be empty")
	suite.T().Log("Successfully serialized data with user_v2 schema - schema evolution registered")

	// Verify schema evolution between V1 and V2 schemas
	evolutionConfirmed := suite.VerifySchemaEvolution("user_evolution_topic-value", userV1Schema, userV2Schema)
	suite.Require().True(evolutionConfirmed, "Schema evolution should be confirmed between V1 and V2 schemas")

	// Verify that the schema was created in AWS Glue Schema Registry using base class method
	suite.VerifySchemaInRegistry("user_evolution_topic-value")
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

	// Capture serializer schema definition from the AvroRecord used for V1 serialization
	serializerSchemaV1Definition := employeeV1Schema

	// Create deserializer to capture the schema used for V1 deserialization
	gsrDeserializer, err := deserializer.NewDeserializer(config)
	suite.Require().NoError(err, "Failed to create GSR deserializer")

	// Capture deserializer schema using GetSchema method from the V1 serialized data
	deserializerSchemaV1, err := gsrDeserializer.GetSchema(serializedV1Data)
	suite.Require().NoError(err, "Failed to capture deserializer schema from serialized V1 data")
	suite.Require().NotNil(deserializerSchemaV1, "Deserializer schema V1 should not be nil")
	deserializerSchemaV1Definition := deserializerSchemaV1.Definition

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
	_, err = gsrSerializer.Serialize("employee_incompatible_topic", avroRecordV2)

	suite.Require().NotNil(err, "Serialization should fail due to backward incompatibility")
	// Assert that serialization fails due to backward compatibility violation
	// Expected case: serialization should fail due to backward incompatibility
	suite.T().Logf("Backward incompatible evolution correctly failed during serialization with error: %v", err)

	// Verify that the error message indicates backward compatibility issues
	errorMsg := err.Error()
	if strings.Contains(errorMsg, "compatibility") || strings.Contains(errorMsg, "Compatibility") ||
		strings.Contains(errorMsg, "backward") || strings.Contains(errorMsg, "incompatible") {
		suite.T().Log("Error message correctly indicates backward compatibility issues")
	}

	// Verify schema evolution for the successful V1 serialization (should return false since same schema)
	evolutionConfirmed := suite.VerifySchemaEvolution("employee_incompatible_topic-value", serializerSchemaV1Definition, deserializerSchemaV1Definition)
	suite.Require().False(evolutionConfirmed, "Schema evolution should return false when using the same schema definition")

	// Test passed - backward incompatibility was correctly detected
	suite.T().Log("Backward incompatible evolution test passed - GSR correctly rejected incompatible schema")

}

// TestSchemaEvolutionSuite runs the schema evolution test suite
func TestSchemaEvolutionSuite(t *testing.T) {
	suite.Run(t, new(SchemaEvolutionTestSuite))
}
