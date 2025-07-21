package avro

import (
	"testing"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// createAvroConfig creates a Configuration object for Avro tests
func createAvroConfig() *common.Configuration {
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatAvro
	return common.NewConfiguration(configMap)
}

// createAvroConfigWithRecordType creates a Configuration object with a specific record type
func createAvroConfigWithRecordType(recordType common.AvroRecordType) *common.Configuration {
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatAvro
	configMap[common.AvroRecordTypeKey] = recordType
	return common.NewConfiguration(configMap)
}

func TestNewAvroSerializer(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)
	if serializer == nil {
		t.Fatal("NewAvroSerializer() returned nil")
	}
}

func TestAvroSerializer_Serialize(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	// Test data - a simple record
	testData := map[string]interface{}{
		"message": "Hello, AVRO!",
	}

	// Test serialization
	serializedData, err := serializer.Serialize(testData)
	if err != nil {
		t.Fatalf("Serialize() failed: %v", err)
	}

	if serializedData == nil {
		t.Fatal("Serialize() returned nil data")
	}

	if len(serializedData) == 0 {
		t.Fatal("Serialize() returned empty data")
	}

	t.Logf("Successfully serialized data: %d bytes", len(serializedData))
}

func TestAvroSerializer_SerializeNilData(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	// Test with nil data
	_, err := serializer.Serialize(nil)
	if err == nil {
		t.Fatal("Expected error for nil data, but got none")
	}

	t.Logf("Got expected error for nil data: %v", err)
}

func TestAvroSerializer_GetSchemaDefinition(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	// Test with a simple record
	testData := map[string]interface{}{
		"name": "John Doe",
		"age":  30,
	}

	schema, err := serializer.GetSchemaDefinition(testData)
	if err != nil {
		t.Fatalf("GetSchemaDefinition() failed: %v", err)
	}

	if schema == "" {
		t.Fatal("GetSchemaDefinition() returned empty schema")
	}

	t.Logf("Generated schema: %s", schema)

	// Test with nil data
	_, err = serializer.GetSchemaDefinition(nil)
	if err == nil {
		t.Fatal("Expected error for nil data, but got none")
	}
}

func TestAvroSerializer_ValidateObject(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	// Test with valid data
	validData := map[string]interface{}{
		"message": "Hello, AVRO!",
	}

	err := serializer.ValidateObject(validData)
	if err != nil {
		t.Fatalf("ValidateObject() failed for valid data: %v", err)
	}

	// Test with nil data
	err = serializer.ValidateObject(nil)
	if err == nil {
		t.Fatal("Expected error for nil data, but got none")
	}

	t.Logf("Got expected error for nil data: %v", err)
}

func TestAvroSerializer_Validate(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	schemaDefinition := `{"type": "record", "name": "TestRecord", "fields": [{"name": "message", "type": "string"}]}`

	// Create some test data
	testData := map[string]interface{}{
		"message": "Hello, AVRO!",
	}

	// Serialize the data first
	serializedData, err := serializer.Serialize(testData)
	if err != nil {
		t.Fatalf("Failed to serialize test data: %v", err)
	}

	// Now validate the serialized data
	err = serializer.Validate(schemaDefinition, serializedData)
	if err != nil {
		t.Fatalf("Validate() failed for valid data: %v", err)
	}

	// Test with empty schema
	err = serializer.Validate("", serializedData)
	if err == nil {
		t.Fatal("Expected error for empty schema, but got none")
	}

	// Test with nil data
	err = serializer.Validate(schemaDefinition, nil)
	if err == nil {
		t.Fatal("Expected error for nil data, but got none")
	}
}

func TestAvroSerializer_SetAdditionalSchemaInfo(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	testData := map[string]interface{}{
		"message": "Hello, AVRO!",
	}

	schema := &gsrserde.Schema{
		Name:       "TestRecord",
		Definition: `{"type": "record", "name": "TestRecord", "fields": [{"name": "message", "type": "string"}]}`,
		DataFormat: "AVRO",
	}

	err := serializer.SetAdditionalSchemaInfo(testData, schema)
	if err != nil {
		t.Fatalf("SetAdditionalSchemaInfo() failed: %v", err)
	}

	// Test with nil data
	err = serializer.SetAdditionalSchemaInfo(nil, schema)
	if err == nil {
		t.Fatal("Expected error for nil data, but got none")
	}

	// Test with nil schema
	err = serializer.SetAdditionalSchemaInfo(testData, nil)
	if err == nil {
		t.Fatal("Expected error for nil schema, but got none")
	}
}
