package avro

import (
	"testing"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// Test schema definition
const testSchema = `{
	"type": "record",
	"name": "TestRecord",
	"fields": [
		{"name": "message", "type": "string"}
	]
}`

const complexTestSchema = `{
	"type": "record",
	"name": "ComplexTestRecord",
	"fields": [
		{"name": "name", "type": "string"},
		{"name": "age", "type": "int"}
	]
}`

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

	// Test data - create proper AvroRecord with schema and Go struct
	testData := map[string]interface{}{
		"message": "Hello, AVRO!",
	}
	avroRecord := avro.NewAvroRecord(testSchema, testData)

	// Test serialization
	serializedData, err := serializer.Serialize(avroRecord)
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

func TestAvroSerializer_SerializeInvalidData(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	// Test with invalid data (not AvroRecord)
	_, err := serializer.Serialize("invalid data")
	if err == nil {
		t.Fatal("Expected error for invalid data, but got none")
	}

	t.Logf("Got expected error for invalid data: %v", err)
}

func TestAvroSerializer_GetSchemaDefinition(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	// Test with a complex record
	testData := map[string]interface{}{
		"name": "John Doe",
		"age":  30,
	}
	avroRecord := avro.NewAvroRecord(complexTestSchema, testData)

	schema, err := serializer.GetSchemaDefinition(avroRecord)
	if err != nil {
		t.Fatalf("GetSchemaDefinition() failed: %v", err)
	}

	if schema == "" {
		t.Fatal("GetSchemaDefinition() returned empty schema")
	}

	if schema != complexTestSchema {
		t.Fatalf("GetSchemaDefinition() returned wrong schema. Expected: %s, Got: %s", complexTestSchema, schema)
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
	avroRecord := avro.NewAvroRecord(testSchema, validData)

	err := serializer.ValidateObject(avroRecord)
	if err != nil {
		t.Fatalf("ValidateObject() failed for valid data: %v", err)
	}

	// Test with nil data
	err = serializer.ValidateObject(nil)
	if err == nil {
		t.Fatal("Expected error for nil data, but got none")
	}

	t.Logf("Got expected error for nil data: %v", err)

	// Test with invalid data type
	err = serializer.ValidateObject("invalid data")
	if err == nil {
		t.Fatal("Expected error for invalid data type, but got none")
	}

	t.Logf("Got expected error for invalid data type: %v", err)
}

func TestAvroSerializer_Validate(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	// Create some test data
	testData := map[string]interface{}{
		"message": "Hello, AVRO!",
	}
	avroRecord := avro.NewAvroRecord(testSchema, testData)

	// Serialize the data first
	serializedData, err := serializer.Serialize(avroRecord)
	if err != nil {
		t.Fatalf("Failed to serialize test data: %v", err)
	}

	// Now validate the serialized data
	err = serializer.Validate(testSchema, serializedData)
	if err != nil {
		t.Fatalf("Validate() failed for valid data: %v", err)
	}

	// Test with empty schema
	err = serializer.Validate("", serializedData)
	if err == nil {
		t.Fatal("Expected error for empty schema, but got none")
	}

	// Test with nil data
	err = serializer.Validate(testSchema, nil)
	if err == nil {
		t.Fatal("Expected error for nil data, but got none")
	}

	// Test with invalid schema
	err = serializer.Validate("invalid schema", serializedData)
	if err == nil {
		t.Fatal("Expected error for invalid schema, but got none")
	}
}

func TestAvroSerializer_SetAdditionalSchemaInfo(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	testData := map[string]interface{}{
		"message": "Hello, AVRO!",
	}
	avroRecord := avro.NewAvroRecord(testSchema, testData)

	schema := &gsrserde.Schema{
		Name:       "TestRecord",
		Definition: "",
		DataFormat: "AVRO",
	}

	err := serializer.SetAdditionalSchemaInfo(avroRecord, schema)
	if err != nil {
		t.Fatalf("SetAdditionalSchemaInfo() failed: %v", err)
	}

	// Check if schema definition was set
	if schema.Definition != testSchema {
		t.Fatalf("Schema definition not set correctly. Expected: %s, Got: %s", testSchema, schema.Definition)
	}

	// Check if additional info was set
	if schema.AdditionalInfo == "" {
		t.Fatal("AdditionalInfo not set")
	}

	t.Logf("AdditionalInfo: %s", schema.AdditionalInfo)

	// Test with nil data
	err = serializer.SetAdditionalSchemaInfo(nil, schema)
	if err == nil {
		t.Fatal("Expected error for nil data, but got none")
	}

	// Test with nil schema
	err = serializer.SetAdditionalSchemaInfo(avroRecord, nil)
	if err == nil {
		t.Fatal("Expected error for nil schema, but got none")
	}
}

func TestAvroSerializer_RoundTrip(t *testing.T) {
	config := createAvroConfig()
	serializer := NewAvroSerializer(config)

	// Create test data
	testData := map[string]interface{}{
		"name": "John Doe",
		"age":  30,
	}
	avroRecord := avro.NewAvroRecord(complexTestSchema, testData)

	// Serialize
	serializedData, err := serializer.Serialize(avroRecord)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Validate serialized data
	err = serializer.Validate(complexTestSchema, serializedData)
	if err != nil {
		t.Fatalf("Serialized data validation failed: %v", err)
	}

	t.Logf("Round-trip test passed: %d bytes serialized", len(serializedData))
}
