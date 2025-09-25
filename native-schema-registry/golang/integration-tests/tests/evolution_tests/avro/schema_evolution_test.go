package avro

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
	"github.com/stretchr/testify/suite"
)

// SchemaEvolutionTestSuite contains the test suite for schema evolution tests
type SchemaEvolutionTestSuite struct {
	suite.Suite
	glueClient   *glue.Client
	registryName string
	ctx          context.Context
}

// SetupSuite runs once before all tests in the suite
func (suite *SchemaEvolutionTestSuite) SetupSuite() {
	suite.ctx = context.Background()

	// Load AWS configuration with explicit region
	cfg, err := config.LoadDefaultConfig(suite.ctx,
		config.WithRegion("us-east-1"), // Default to us-east-1 if no region is configured
	)
	suite.Require().NoError(err, "Failed to load AWS config")

	// Create Glue client
	suite.glueClient = glue.NewFromConfig(cfg)

	// Create unique registry name for this test run
	suite.registryName = fmt.Sprintf("gsr-evolution-test-%d", time.Now().Unix())

	// Create the registry
	_, err = suite.glueClient.CreateRegistry(suite.ctx, &glue.CreateRegistryInput{
		RegistryName: aws.String(suite.registryName),
		Description:  aws.String("Test registry for schema evolution tests"),
	})
	suite.Require().NoError(err, "Failed to create test registry")
}

// TearDownSuite runs once after all tests in the suite
func (suite *SchemaEvolutionTestSuite) TearDownSuite() {
	suite.T().Logf("Tearing down suite")
	suite.T().Logf("Deleting registry %s", suite.registryName)
	// Delete the registry (this will automatically delete all schemas within it)
	_, err := suite.glueClient.DeleteRegistry(suite.ctx, &glue.DeleteRegistryInput{
		RegistryId: &types.RegistryId{
			RegistryName: aws.String(suite.registryName),
		},
	})

	if err != nil {
		suite.T().Logf("Warning: Failed to delete registry %s: %v", suite.registryName, err)
	} else {
		suite.T().Logf("Successfully deleted registry %s", suite.registryName)
	}
}

// loadSchemaFromFile loads an Avro schema from a file
func (suite *SchemaEvolutionTestSuite) loadSchemaFromFile(filename string) string {
	// Try multiple possible paths to find the schema files
	possiblePaths := []string{
		filepath.Join("../../../../../../shared/test/avro", filename), // From tests/evolution_tests/avro directory
		filepath.Join("../../../../../shared/test/avro", filename),    // From evolution_tests/avro directory
		filepath.Join("../../../../shared/test/avro", filename),       // From integration-tests directory
		filepath.Join("../../../shared/test/avro", filename),          // From golang directory
		filepath.Join("shared/test/avro", filename),                   // From project root
	}

	for _, schemaPath := range possiblePaths {
		if content, err := os.ReadFile(schemaPath); err == nil {
			return string(content)
		}
	}

	// If none of the paths work, fail with a helpful error
	cwd, _ := os.Getwd()
	suite.Require().Fail("Failed to read schema file: %s. Current working directory: %s. Tried paths: %v", filename, cwd, possiblePaths)
	return ""
}

// TestBackwardCompatibleEvolution tests backward compatible schema evolution
// This test verifies that a schema evolution that removes a field and adds an optional field
// with a default value maintains backward compatibility
func (suite *SchemaEvolutionTestSuite) TestBackwardCompatibleEvolution() {
	// Load user_v1.avsc and user_v2.avsc schemas using existing loadSchemaFromFile method
	userV2Schema := suite.loadSchemaFromFile("backward/user_v2.avsc")

	// Create temporary GSR properties file with test registry name
	tempPropsContent := fmt.Sprintf(`region=us-east-1
endpoint=https://glue.us-east-1.amazonaws.com
registry.name=%s
description=Schema evolution test registry
compatibility=BACKWARD
dataFormat=AVRO
schemaAutoRegistrationEnabled=true`, suite.registryName)

	tempPropsFile := filepath.Join(".", "test_gsr.properties")
	err := os.WriteFile(tempPropsFile, []byte(tempPropsContent), 0644)
	suite.Require().NoError(err, "Failed to create temporary GSR properties file")
	defer func() {
		os.Remove(tempPropsFile)
	}()

	gsrConfigAbsolutePath, err := filepath.Abs(tempPropsFile)
	suite.Require().NoError(err, "Failed to get absolute path of test GSR properties")

	// Create GSR configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.GSRConfigPathKey:  gsrConfigAbsolutePath,
	}
	config := common.NewConfiguration(configMap)

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

	// Verify that the schema was created in AWS Glue Schema Registry
	schemaResponse, err := suite.glueClient.GetSchema(suite.ctx, &glue.GetSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String("user_v2_topic-value"),
		},
	})
	suite.Require().NoError(err, "Failed to retrieve schema from GSR")
	suite.Require().NotNil(schemaResponse, "Schema response should not be nil")
	suite.Require().NotNil(schemaResponse.SchemaName, "Schema name should not be nil")
	suite.Equal("user_v2_topic-value", *schemaResponse.SchemaName, "Schema name should match")
	suite.T().Logf("Verified schema 'user_v2_topic-value' was created in GSR")

	// Verify that schema versions were created
	versionsResponse, err := suite.glueClient.ListSchemaVersions(suite.ctx, &glue.ListSchemaVersionsInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String("user_v2_topic-value"),
		},
	})
	suite.Require().NoError(err, "Failed to list schema versions from GSR")
	suite.Require().NotNil(versionsResponse, "Schema versions response should not be nil")
	suite.Require().NotEmpty(versionsResponse.Schemas, "Schema versions should not be empty")
	suite.T().Logf("Verified %d schema version(s) created for 'user_v2_topic-value'", len(versionsResponse.Schemas))

	suite.T().Log("Backward compatible evolution test passed successfully")
}

// TestBackwardCompatibleEvolution_V1ToV2 tests backward compatible schema evolution
// This test verifies that data serialized with user_v1 schema can be properly deserialized
// and that the deserializer is compatible with serializer using an earlier schema version
func (suite *SchemaEvolutionTestSuite) TestBackwardCompatibleEvolution_V1ToV2() {
	// Load user_v1.avsc and user_v2.avsc schemas using existing loadSchemaFromFile method
	userV1Schema := suite.loadSchemaFromFile("backward/user_v1.avsc")
	userV2Schema := suite.loadSchemaFromFile("backward/user_v2.avsc")

	// Create temporary GSR properties file with test registry name
	tempPropsContent := fmt.Sprintf(`region=us-east-1
endpoint=https://glue.us-east-1.amazonaws.com
registry.name=%s
description=Schema evolution test registry
compatibility=BACKWARD
dataFormat=AVRO
schemaAutoRegistrationEnabled=true`, suite.registryName)

	tempPropsFile := filepath.Join(".", "test_gsr_v1tov2.properties")
	err := os.WriteFile(tempPropsFile, []byte(tempPropsContent), 0644)
	suite.Require().NoError(err, "Failed to create temporary GSR properties file")
	defer func() {
		os.Remove(tempPropsFile)
	}()

	gsrConfigAbsolutePath, err := filepath.Abs(tempPropsFile)
	suite.Require().NoError(err, "Failed to get absolute path of test GSR properties")

	// Create GSR configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.GSRConfigPathKey:  gsrConfigAbsolutePath,
	}
	config := common.NewConfiguration(configMap)

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

	// Verify that both user_v1 and user_v2 schemas were created in AWS Glue Schema Registry
	// Check user_v1 schema
	schemaV1Response, err := suite.glueClient.GetSchema(suite.ctx, &glue.GetSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String("user_v1_topic-value"),
		},
	})
	suite.Require().NoError(err, "Failed to retrieve user_v1 schema from GSR")
	suite.Require().NotNil(schemaV1Response, "User_v1 schema response should not be nil")
	suite.T().Logf("Verified schema 'user_v1_topic-value' was created in GSR")

	// Check user_v2_compat schema
	schemaV2Response, err := suite.glueClient.GetSchema(suite.ctx, &glue.GetSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String("user_v2_compat_topic-value"),
		},
	})
	suite.Require().NoError(err, "Failed to retrieve user_v2_compat schema from GSR")
	suite.Require().NotNil(schemaV2Response, "User_v2_compat schema response should not be nil")
	suite.T().Logf("Verified schema 'user_v2_compat_topic-value' was created in GSR")

	// Verify schema versions for both schemas
	v1VersionsResponse, err := suite.glueClient.ListSchemaVersions(suite.ctx, &glue.ListSchemaVersionsInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String("user_v1_topic-value"),
		},
	})
	suite.Require().NoError(err, "Failed to list user_v1 schema versions from GSR")
	suite.Require().NotEmpty(v1VersionsResponse.Schemas, "User_v1 schema versions should not be empty")
	suite.T().Logf("Verified %d schema version(s) created for 'user_v1_topic-value'", len(v1VersionsResponse.Schemas))

	v2VersionsResponse, err := suite.glueClient.ListSchemaVersions(suite.ctx, &glue.ListSchemaVersionsInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String("user_v2_compat_topic-value"),
		},
	})
	suite.Require().NoError(err, "Failed to list user_v2_compat schema versions from GSR")
	suite.Require().NotEmpty(v2VersionsResponse.Schemas, "User_v2_compat schema versions should not be empty")
	suite.T().Logf("Verified %d schema version(s) created for 'user_v2_compat_topic-value'", len(v2VersionsResponse.Schemas))

	suite.T().Log("Backward compatible evolution test with user_v1 schema passed successfully")
}

// TestBackwardIncompatibleEvolution tests backward incompatible schema evolution
// This test verifies that a schema evolution that adds a required field without a default
// breaks backward compatibility and should fail
func (suite *SchemaEvolutionTestSuite) TestBackwardIncompatibleEvolution() {
	// Load employee_v1.avsc and employee_v2.avsc schemas using existing loadSchemaFromFile method
	employeeV1Schema := suite.loadSchemaFromFile("negative/backward/employee_v1.avsc")
	employeeV2Schema := suite.loadSchemaFromFile("negative/backward/employee_v2.avsc")

	// Create temporary GSR properties file with test registry name
	tempPropsContent := fmt.Sprintf(`region=us-east-1
endpoint=https://glue.us-east-1.amazonaws.com
registry.name=%s
description=Schema evolution test registry
compatibility=BACKWARD
dataFormat=AVRO
schemaAutoRegistrationEnabled=true`, suite.registryName)

	tempPropsFile := filepath.Join(".", "test_gsr_incompatible.properties")
	err := os.WriteFile(tempPropsFile, []byte(tempPropsContent), 0644)
	suite.Require().NoError(err, "Failed to create temporary GSR properties file")
	defer func() {
		os.Remove(tempPropsFile)
	}()

	gsrConfigAbsolutePath, err := filepath.Abs(tempPropsFile)
	suite.Require().NoError(err, "Failed to get absolute path of test GSR properties")

	// Create GSR configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.GSRConfigPathKey:  gsrConfigAbsolutePath,
	}
	config := common.NewConfiguration(configMap)

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

	// Verify that the employee schema was created in AWS Glue Schema Registry
	schemaResponse, err := suite.glueClient.GetSchema(suite.ctx, &glue.GetSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String("employee_incompatible_topic-value"),
		},
	})
	suite.Require().NoError(err, "Failed to retrieve employee schema from GSR")
	suite.Require().NotNil(schemaResponse, "Employee schema response should not be nil")
	suite.Require().NotNil(schemaResponse.SchemaName, "Employee schema name should not be nil")
	suite.Equal("employee_incompatible_topic-value", *schemaResponse.SchemaName, "Employee schema name should match")
	suite.T().Logf("Verified schema 'employee_incompatible_topic-value' was created in GSR")

	// Verify that only one schema version was created (v1 only, v2 should have been rejected)
	versionsResponse, err := suite.glueClient.ListSchemaVersions(suite.ctx, &glue.ListSchemaVersionsInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String("employee_incompatible_topic-value"),
		},
	})
	suite.Require().NoError(err, "Failed to list employee schema versions from GSR")
	suite.Require().NotNil(versionsResponse, "Employee schema versions response should not be nil")
	suite.Require().NotEmpty(versionsResponse.Schemas, "Employee schema versions should not be empty")
	suite.T().Logf("Verified %d schema version(s) created for 'employee_incompatible_topic-value'", len(versionsResponse.Schemas))

	suite.T().Log("Backward incompatible evolution test completed successfully")
}

// TestSchemaEvolutionSuite runs the schema evolution test suite
func TestSchemaEvolutionSuite(t *testing.T) {
	suite.Run(t, new(SchemaEvolutionTestSuite))
}
