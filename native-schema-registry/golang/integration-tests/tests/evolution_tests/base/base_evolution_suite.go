package base

import (
	"context"
	"embed"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/stretchr/testify/suite"
)

// Embed schema files at compile time
//
//go:embed schemas/avro/*
var avroSchemas embed.FS

//go:embed schemas/json/*
var jsonSchemas embed.FS

// BaseEvolutionTestSuite provides common functionality for schema evolution tests
type BaseEvolutionTestSuite struct {
	suite.Suite
	glueClient   *glue.Client
	registryName string
	ctx          context.Context
	region       string
}

// SetupSuite runs once before all tests in the suite
// Initializes AWS client and creates unique registry for testing
func (suite *BaseEvolutionTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	suite.region = "us-east-1" // Default region

	// Override region if AWS_REGION environment variable is set
	if envRegion := os.Getenv("AWS_REGION"); envRegion != "" {
		suite.region = envRegion
	}

	suite.T().Logf("Setting up evolution test suite with region: %s", suite.region)

	// Load AWS configuration with explicit region
	cfg, err := config.LoadDefaultConfig(suite.ctx,
		config.WithRegion(suite.region),
	)
	suite.Require().NoError(err, "Failed to load AWS config")

	// Create Glue client
	suite.glueClient = glue.NewFromConfig(cfg)

	// Create unique registry name for this test run
	suite.registryName = fmt.Sprintf("gsr-evolution-test-%d", time.Now().Unix())
	suite.T().Logf("Creating test registry: %s", suite.registryName)

	// Create the registry
	_, err = suite.glueClient.CreateRegistry(suite.ctx, &glue.CreateRegistryInput{
		RegistryName: aws.String(suite.registryName),
		Description:  aws.String("Test registry for schema evolution tests"),
	})
	suite.Require().NoError(err, "Failed to create test registry: %s", suite.registryName)
	suite.T().Logf("Successfully created test registry: %s", suite.registryName)
}

// TearDownSuite runs once after all tests in the suite
// Cleans up registry and all associated schemas
func (suite *BaseEvolutionTestSuite) TearDownSuite() {
	if suite.registryName == "" {
		suite.T().Log("No registry to clean up")
		return
	}

	suite.T().Logf("Tearing down evolution test suite")
	suite.T().Logf("Deleting registry: %s", suite.registryName)

	// Delete the registry (this will automatically delete all schemas within it)
	_, err := suite.glueClient.DeleteRegistry(suite.ctx, &glue.DeleteRegistryInput{
		RegistryId: &types.RegistryId{
			RegistryName: aws.String(suite.registryName),
		},
	})

	if err != nil {
		suite.T().Logf("Warning: Failed to delete registry %s: %v", suite.registryName, err)
	} else {
		suite.T().Logf("Successfully deleted registry: %s", suite.registryName)
	}
}

// LoadSchemaFromFile loads a schema from embedded files
// filename: the schema file path relative to the format directory (e.g., "backward/user_v1.avsc", "user.json")
// format: the schema format (use common.DataFormatAvro.String() or common.DataFormatJSON.String())
// Note: Protobuf schemas are not loaded via this method as they are compiled to .go files
func (suite *BaseEvolutionTestSuite) LoadSchemaFromFile(filename, format string) string {
	var content []byte
	var err error
	var embedPath string

	switch format {
	case common.DataFormatAvro.String():
		embedPath = fmt.Sprintf("schemas/avro/%s", filename)
		content, err = avroSchemas.ReadFile(embedPath)
	case common.DataFormatJSON.String():
		embedPath = fmt.Sprintf("schemas/json/%s", filename)
		content, err = jsonSchemas.ReadFile(embedPath)
	default:
		suite.Require().Fail("Unsupported schema format: %s. Supported formats: %s, %s",
			format, common.DataFormatAvro.String(), common.DataFormatJSON.String())
		return ""
	}

	if err != nil {
		suite.Require().Fail("Failed to read embedded schema file: %s (format: %s, embed path: %s). Error: %v",
			filename, format, embedPath, err)
		return ""
	}

	suite.T().Logf("Successfully loaded embedded schema: %s (format: %s)", filename, format)
	return string(content)
}

// CreateGSRConfig creates a GSR configuration for the specified data format
func (suite *BaseEvolutionTestSuite) CreateGSRConfig(dataFormat string) *common.Configuration {
	// Create temporary GSR properties file with test registry name
	tempPropsContent := fmt.Sprintf(`region=%s
endpoint=https://glue.%s.amazonaws.com
registry.name=%s
description=Schema evolution test registry
compatibility=BACKWARD
dataFormat=%s
schemaAutoRegistrationEnabled=true`,
		suite.region, suite.region, suite.registryName, dataFormat)

	tempPropsFile := filepath.Join(".", fmt.Sprintf("test_gsr_%s.properties", dataFormat))
	err := os.WriteFile(tempPropsFile, []byte(tempPropsContent), 0644)
	suite.Require().NoError(err, "Failed to create temporary GSR properties file for %s", dataFormat)

	// Schedule cleanup of temporary file
	suite.T().Cleanup(func() {
		if err := os.Remove(tempPropsFile); err != nil {
			suite.T().Logf("Warning: Failed to remove temporary properties file %s: %v", tempPropsFile, err)
		}
	})

	gsrConfigAbsolutePath, err := filepath.Abs(tempPropsFile)
	suite.Require().NoError(err, "Failed to get absolute path of test GSR properties")

	// Determine data format type
	var dataFormatType common.DataFormat
	switch dataFormat {
	case "AVRO":
		dataFormatType = common.DataFormatAvro
	case "JSON":
		dataFormatType = common.DataFormatJSON
	case "PROTOBUF":
		dataFormatType = common.DataFormatProtobuf
	default:
		suite.Require().Fail("Unsupported data format: %s. Supported formats: AVRO, JSON, PROTOBUF", dataFormat)
		return nil
	}

	// Create GSR configuration
	configMap := map[string]interface{}{
		common.DataFormatTypeKey: dataFormatType,
		common.GSRConfigPathKey:  gsrConfigAbsolutePath,
	}

	config := common.NewConfiguration(configMap)
	suite.T().Logf("Created GSR configuration for %s format with registry: %s", dataFormat, suite.registryName)
	return config
}

// VerifySchemaInRegistry verifies that a schema exists in the AWS Glue Schema Registry
func (suite *BaseEvolutionTestSuite) VerifySchemaInRegistry(schemaName string) {
	suite.T().Logf("Verifying schema '%s' exists in registry '%s'", schemaName, suite.registryName)

	schemaResponse, err := suite.glueClient.GetSchema(suite.ctx, &glue.GetSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String(schemaName),
		},
	})
	suite.Require().NoError(err, "Failed to retrieve schema '%s' from GSR", schemaName)
	suite.Require().NotNil(schemaResponse, "Schema response should not be nil for schema '%s'", schemaName)
	suite.Require().NotNil(schemaResponse.SchemaName, "Schema name should not be nil for schema '%s'", schemaName)
	suite.Equal(schemaName, *schemaResponse.SchemaName, "Schema name should match for schema '%s'", schemaName)

	suite.T().Logf("Successfully verified schema '%s' exists in GSR", schemaName)

	// Also verify schema versions
	versionsResponse, err := suite.glueClient.ListSchemaVersions(suite.ctx, &glue.ListSchemaVersionsInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(suite.registryName),
			SchemaName:   aws.String(schemaName),
		},
	})
	suite.Require().NoError(err, "Failed to list schema versions for '%s' from GSR", schemaName)
	suite.Require().NotNil(versionsResponse, "Schema versions response should not be nil for schema '%s'", schemaName)
	suite.Require().NotEmpty(versionsResponse.Schemas, "Schema versions should not be empty for schema '%s'", schemaName)

	suite.T().Logf("Verified %d schema version(s) exist for '%s'", len(versionsResponse.Schemas), schemaName)
}

// GetRegistryName returns the current test registry name
func (suite *BaseEvolutionTestSuite) GetRegistryName() string {
	return suite.registryName
}

// GetRegion returns the current AWS region
func (suite *BaseEvolutionTestSuite) GetRegion() string {
	return suite.region
}

// GetGlueClient returns the AWS Glue client
func (suite *BaseEvolutionTestSuite) GetGlueClient() *glue.Client {
	return suite.glueClient
}

// GetContext returns the context
func (suite *BaseEvolutionTestSuite) GetContext() context.Context {
	return suite.ctx
}

// VerifySchemaEvolution verifies that different schema versions were used for serialization and deserialization
// and confirms that backward compatibility was properly handled between schema versions
// Returns true if both schemas are available and have different version IDs
// Returns false if either schema failed, or if the two IDs are the same
// Throws an error if any schema status is deleted
// Retries with exponential backoff if either schema is pending (max 4 retries, starting at 1 second)
func (suite *BaseEvolutionTestSuite) VerifySchemaEvolution(schemaName, firstSchemaDefinition, secondSchemaDefinition string) bool {
	suite.T().Logf("Verifying schema evolution between first and second schema definitions")

	// Verify that schema definitions are not empty
	suite.Require().NotEmpty(firstSchemaDefinition, "First schema definition should not be empty")
	suite.Require().NotEmpty(secondSchemaDefinition, "Second schema definition should not be empty")

	// Log schema definitions for debugging (truncated for readability)
	suite.T().Logf("First schema definition (first 100 chars): %.100s...", firstSchemaDefinition)
	suite.T().Logf("Second schema definition (first 100 chars): %.100s...", secondSchemaDefinition)

	const maxRetries = 4
	const initialBackoffSeconds = 1

	for attempt := 0; attempt <= maxRetries; attempt++ {
		suite.T().Logf("Schema evolution verification attempt %d/%d", attempt+1, maxRetries+1)

		// Fetch schema information from AWS Glue
		firstSchemaResponse, err := suite.glueClient.GetSchemaByDefinition(suite.ctx, &glue.GetSchemaByDefinitionInput{
			SchemaId: &types.SchemaId{
				RegistryName: aws.String(suite.registryName),
				SchemaName:   aws.String(schemaName),
			},
			SchemaDefinition: aws.String(firstSchemaDefinition),
		})
		suite.Require().NoError(err, "Failed to retrieve first schema by definition from GSR")
		suite.Require().NotNil(firstSchemaResponse, "First schema response should not be nil")

		secondSchemaResponse, err := suite.glueClient.GetSchemaByDefinition(suite.ctx, &glue.GetSchemaByDefinitionInput{
			SchemaId: &types.SchemaId{
				RegistryName: aws.String(suite.registryName),
				SchemaName:   aws.String(schemaName),
			},
			SchemaDefinition: aws.String(secondSchemaDefinition),
		})
		suite.Require().NoError(err, "Failed to retrieve second schema by definition from GSR")
		suite.Require().NotNil(secondSchemaResponse, "Second schema response should not be nil")

		// Log schema version information
		if firstSchemaResponse.SchemaVersionId != nil {
			suite.T().Logf("First schema version ID: %s", *firstSchemaResponse.SchemaVersionId)
		}
		if secondSchemaResponse.SchemaVersionId != nil {
			suite.T().Logf("Second schema version ID: %s", *secondSchemaResponse.SchemaVersionId)
		}

		// Check if we have valid schema version IDs
		if firstSchemaResponse.SchemaVersionId == nil || secondSchemaResponse.SchemaVersionId == nil {
			suite.T().Log("Warning: Could not compare schema version IDs (one or both are nil)")
			return false
		}

		// Check schema statuses first
		firstStatus := firstSchemaResponse.Status
		secondStatus := secondSchemaResponse.Status

		suite.T().Logf("First schema status: %s, Second schema status: %s", firstStatus, secondStatus)

		// Throw error if any schema status is deleted
		if firstStatus == types.SchemaVersionStatusDeleting || secondStatus == types.SchemaVersionStatusDeleting {
			suite.Require().Fail("One or both schemas have been deleted - cannot verify schema evolution")
			return false
		}
		// Return false if either schema failed
		if firstStatus == types.SchemaVersionStatusFailure || secondStatus == types.SchemaVersionStatusFailure {
			suite.T().Log("One or both schemas failed - schema evolution verification failed")
			return false
		}
		// Check if schemas are the same (no evolution) - return false
		if *firstSchemaResponse.SchemaVersionId == *secondSchemaResponse.SchemaVersionId {
			suite.T().Logf("Same schema version used for both schemas: %s", *firstSchemaResponse.SchemaVersionId)
			suite.T().Log("No schema evolution detected - same schema version used")
			return false
		}
		// Log different schema versions detected
		suite.T().Logf("Different schema versions detected - First: %s, Second: %s",
			*firstSchemaResponse.SchemaVersionId, *secondSchemaResponse.SchemaVersionId)
		// Return true if both schemas are available and IDs are different
		if firstStatus == types.SchemaVersionStatusAvailable && secondStatus == types.SchemaVersionStatusAvailable {
			suite.T().Log("Both schemas are available and have different version IDs - schema evolution confirmed")
			return true
		}

		// If either schema is pending and we haven't reached max retries, wait and retry
		if (firstStatus == types.SchemaVersionStatusPending || secondStatus == types.SchemaVersionStatusPending) && attempt < maxRetries {
			backoffDuration := time.Duration(math.Pow(2, float64(attempt))) * time.Duration(initialBackoffSeconds) * time.Second
			suite.T().Logf("One or both schemas are pending, retrying in %v (attempt %d/%d)", backoffDuration, attempt+1, maxRetries+1)
			time.Sleep(backoffDuration)
			continue
		}

		// If we reach here, schemas are not available and not pending (or max retries reached)
		suite.T().Log("Schemas are not available and not pending, or max retries reached")
		return false
	}

	suite.T().Log("Max retries reached - schema evolution verification failed")
	return false
}
