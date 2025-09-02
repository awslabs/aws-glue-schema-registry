package negative_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb/syntax2/basic"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

type ConfigurationTestSuite struct {
	suite.Suite
}

const (
	MISSING_PATH           = "missing_path"
	INVALID_ROLE_TO_ASSUME = "./configs/invalid_role_to_assume.properties"
	NON_EXISTANT_REGISTRY  = "./configs/non_existant_registry.properties"
	NON_EXISTANT_ENDPOINT  = "./configs/non_existant_endpoint.properties"
	NON_EXISTANT_REGION    = "./configs/non_existant_region.properties"
)

// createTestPhone creates a valid Phone protobuf message for testing
func (c *ConfigurationTestSuite) createTestPhone() *basic.Phone {
	return &basic.Phone{
		Model:  proto.String("iPhone 15"),
		Name:   proto.String("Test Phone"),
		Serial: proto.Int32(12345),
	}
}

// createProtobufConfig creates a protobuf configuration with the given config path
func (c *ConfigurationTestSuite) createProtobufConfig(configPath string) *common.Configuration {
	phone := c.createTestPhone()
	messageDescriptor := phone.ProtoReflect().Descriptor()

	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: messageDescriptor,
		common.GSRConfigPathKey:             configPath,
	}

	return common.NewConfiguration(configMap)
}

// TestSerializerWithMissingPath tests serializer creation with missing config path
func (c *ConfigurationTestSuite) TestSerializerWithMissingPath() {
	c.T().Log("Testing serializer with missing configuration path...")

	config := c.createProtobufConfig(MISSING_PATH)
	phone := c.createTestPhone()

	// Attempt to create serializer with missing config path
	gsrSerializer, err := serializer.NewSerializer(config)
	if err != nil {
		// Expected: serializer creation should fail
		c.T().Logf("✓ Serializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Serializer creation should fail with missing config path")
		return
	}

	// If serializer creation succeeds, try to serialize and expect it to fail
	defer gsrSerializer.Close()

	transportName := "test-transport"
	_, serializeErr := gsrSerializer.Serialize(transportName, phone)
	require.Error(c.T(), serializeErr, "Serialize should fail with missing config path")
	c.T().Logf("✓ Serialize operation failed as expected: %v", serializeErr)
}

// TestSerializerWithInvalidRoleToAssume tests serializer with invalid role configuration
func (c *ConfigurationTestSuite) TestSerializerWithInvalidRoleToAssume() {
	c.T().Log("Testing serializer with invalid role to assume...")

	config := c.createProtobufConfig(INVALID_ROLE_TO_ASSUME)
	phone := c.createTestPhone()

	gsrSerializer, err := serializer.NewSerializer(config)
	if err != nil {
		c.T().Logf("✓ Serializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Serializer creation should fail with invalid role")
		return
	}

	defer gsrSerializer.Close()

	transportName := "test-transport"
	_, serializeErr := gsrSerializer.Serialize(transportName, phone)
	require.Error(c.T(), serializeErr, "Serialize should fail with invalid role configuration")
	c.T().Logf("✓ Serialize operation failed as expected: %v", serializeErr)
}

// TestSerializerWithNonExistentRegistry tests serializer with non-existent registry
func (c *ConfigurationTestSuite) TestSerializerWithNonExistentRegistry() {
	c.T().Log("Testing serializer with non-existent registry...")

	config := c.createProtobufConfig(NON_EXISTANT_REGISTRY)
	phone := c.createTestPhone()

	gsrSerializer, err := serializer.NewSerializer(config)
	if err != nil {
		c.T().Logf("✓ Serializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Serializer creation should fail with non-existent registry")
		return
	}

	defer gsrSerializer.Close()

	transportName := "test-transport"
	_, serializeErr := gsrSerializer.Serialize(transportName, phone)
	require.Error(c.T(), serializeErr, "Serialize should fail with non-existent registry")
	c.T().Logf("✓ Serialize operation failed as expected: %v", serializeErr)
}

// TestSerializerWithNonExistentEndpoint tests serializer with non-existent endpoint
func (c *ConfigurationTestSuite) TestSerializerWithNonExistentEndpoint() {
	c.T().Log("Testing serializer with non-existent endpoint...")

	config := c.createProtobufConfig(NON_EXISTANT_ENDPOINT)
	phone := c.createTestPhone()

	gsrSerializer, err := serializer.NewSerializer(config)
	if err != nil {
		c.T().Logf("✓ Serializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Serializer creation should fail with non-existent endpoint")
		return
	}

	defer gsrSerializer.Close()

	transportName := "test-transport"
	_, serializeErr := gsrSerializer.Serialize(transportName, phone)
	require.Error(c.T(), serializeErr, "Serialize should fail with non-existent endpoint")
	c.T().Logf("✓ Serialize operation failed as expected: %v", serializeErr)
}

// TestSerializerWithNonExistentRegion tests serializer with non-existent region
func (c *ConfigurationTestSuite) TestSerializerWithNonExistentRegion() {
	c.T().Log("Testing serializer with non-existent region...")

	config := c.createProtobufConfig(NON_EXISTANT_REGION)
	phone := c.createTestPhone()

	gsrSerializer, err := serializer.NewSerializer(config)
	if err != nil {
		c.T().Logf("✓ Serializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Serializer creation should fail with non-existent region")
		return
	}

	defer gsrSerializer.Close()

	transportName := "test-transport"
	_, serializeErr := gsrSerializer.Serialize(transportName, phone)
	require.Error(c.T(), serializeErr, "Serialize should fail with non-existent region")
	c.T().Logf("✓ Serialize operation failed as expected: %v", serializeErr)
}

// TestDeserializerWithMissingPath tests deserializer creation with missing config path
func (c *ConfigurationTestSuite) TestDeserializerWithMissingPath() {
	c.T().Log("Testing deserializer with missing configuration path...")

	config := c.createProtobufConfig(MISSING_PATH)

	// Attempt to create deserializer with missing config path
	gsrDeserializer, err := deserializer.NewDeserializer(config)
	if err != nil {
		c.T().Logf("✓ Deserializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Deserializer creation should fail with missing config path")
		return
	}

	defer gsrDeserializer.Close()

	// Try to use deserializer methods with dummy data and expect failures
	dummyData := []byte{0x01, 0x02, 0x03, 0x04}

	_, canDecodeErr := gsrDeserializer.CanDeserialize(dummyData)
	require.Error(c.T(), canDecodeErr, "CanDeserialize should fail with missing config")
	c.T().Logf("✓ CanDeserialize failed as expected: %v", canDecodeErr)

	_, decodeErr := gsrDeserializer.Deserialize("test-topic", dummyData)
	require.Error(c.T(), decodeErr, "Deserialize should fail with missing config")
	c.T().Logf("✓ Deserialize failed as expected: %v", decodeErr)
}

// TestDeserializerWithInvalidRoleToAssume tests deserializer with invalid role configuration
func (c *ConfigurationTestSuite) TestDeserializerWithInvalidRoleToAssume() {
	c.T().Log("Testing deserializer with invalid role to assume...")

	config := c.createProtobufConfig(INVALID_ROLE_TO_ASSUME)

	gsrDeserializer, err := deserializer.NewDeserializer(config)
	if err != nil {
		c.T().Logf("✓ Deserializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Deserializer creation should fail with invalid role")
		return
	}

	defer gsrDeserializer.Close()

	dummyData := []byte{0x01, 0x02, 0x03, 0x04}
	_, decodeErr := gsrDeserializer.Deserialize("test-topic", dummyData)
	require.Error(c.T(), decodeErr, "Deserialize should fail with invalid role configuration")
	c.T().Logf("✓ Deserialize operation failed as expected: %v", decodeErr)
}

// TestDeserializerWithNonExistentRegistry tests deserializer with non-existent registry
func (c *ConfigurationTestSuite) TestDeserializerWithNonExistentRegistry() {
	c.T().Log("Testing deserializer with non-existent registry...")

	config := c.createProtobufConfig(NON_EXISTANT_REGISTRY)

	gsrDeserializer, err := deserializer.NewDeserializer(config)
	if err != nil {
		c.T().Logf("✓ Deserializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Deserializer creation should fail with non-existent registry")
		return
	}

	defer gsrDeserializer.Close()

	dummyData := []byte{0x01, 0x02, 0x03, 0x04}
	_, decodeErr := gsrDeserializer.Deserialize("test-topic", dummyData)
	require.Error(c.T(), decodeErr, "Deserialize should fail with non-existent registry")
	c.T().Logf("✓ Deserialize operation failed as expected: %v", decodeErr)
}

// TestDeserializerWithNonExistentEndpoint tests deserializer with non-existent endpoint
func (c *ConfigurationTestSuite) TestDeserializerWithNonExistentEndpoint() {
	c.T().Log("Testing deserializer with non-existent endpoint...")

	config := c.createProtobufConfig(NON_EXISTANT_ENDPOINT)

	gsrDeserializer, err := deserializer.NewDeserializer(config)
	if err != nil {
		c.T().Logf("✓ Deserializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Deserializer creation should fail with non-existent endpoint")
		return
	}

	defer gsrDeserializer.Close()

	dummyData := []byte{0x01, 0x02, 0x03, 0x04}
	_, decodeErr := gsrDeserializer.Deserialize("test-topic", dummyData)
	require.Error(c.T(), decodeErr, "Deserialize should fail with non-existent endpoint")
	c.T().Logf("✓ Deserialize operation failed as expected: %v", decodeErr)
}

// TestDeserializerWithNonExistentRegion tests deserializer with non-existent region
func (c *ConfigurationTestSuite) TestDeserializerWithNonExistentRegion() {
	c.T().Log("Testing deserializer with non-existent region...")

	config := c.createProtobufConfig(NON_EXISTANT_REGION)

	gsrDeserializer, err := deserializer.NewDeserializer(config)
	if err != nil {
		c.T().Logf("✓ Deserializer creation failed as expected: %v", err)
		require.Error(c.T(), err, "Deserializer creation should fail with non-existent region")
		return
	}

	defer gsrDeserializer.Close()

	dummyData := []byte{0x01, 0x02, 0x03, 0x04}
	_, decodeErr := gsrDeserializer.Deserialize("test-topic", dummyData)
	require.Error(c.T(), decodeErr, "Deserialize should fail with non-existent region")
	c.T().Logf("✓ Deserialize operation failed as expected: %v", decodeErr)
}

// TestConfigurationTestSuite runs the configuration negative test suite
func TestConfigurationTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigurationTestSuite))
}
