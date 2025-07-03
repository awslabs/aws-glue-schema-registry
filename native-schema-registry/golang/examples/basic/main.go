package main

import (
	"fmt"
	"log"
	"unsafe"

	"github.com/awslabs/aws-glue-schema-registry/golang/pkg/gsrserde"
)

func main() {
	fmt.Println("=== AWS Glue Schema Registry Go Bindings Example ===")

	// Create a schema
	schemaName := "user-schema"
	schemaDef := `{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}]}`
	dataFormat := "AVRO"
	additionalInfo := ""

	// Error handling - in production, implement proper error handling
	var err gsrserde.Glue_schema_registry_error

	fmt.Printf("Creating schema: %s\n", schemaName)
	schema := gsrserde.NewGlue_schema_registry_schema(schemaName, schemaDef, dataFormat, additionalInfo, err)
	defer gsrserde.DeleteGlue_schema_registry_schema(schema)

	// Verify schema properties
	retrievedName := schema.Get_schema_name()
	retrievedDef := schema.Get_schema_def()
	retrievedFormat := schema.Get_data_format()

	fmt.Printf("Schema Name: %s\n", retrievedName)
	fmt.Printf("Schema Format: %s\n", retrievedFormat)
	fmt.Printf("Schema Definition: %s\n", retrievedDef)

	// Create serializer and deserializer
	fmt.Println("\nCreating serializer and deserializer...")
	serializer := gsrserde.NewGlue_schema_registry_serializer(err)
	defer gsrserde.DeleteGlue_schema_registry_serializer(serializer)

	deserializer := gsrserde.NewGlue_schema_registry_deserializer(err)
	defer gsrserde.DeleteGlue_schema_registry_deserializer(deserializer)

	// Example data to serialize
	testData := []byte(`{"name": "John", "age": 30}`)
	fmt.Printf("Test data: %s\n", string(testData))

	// Create read-only byte array from test data
	readOnlyArray := gsrserde.NewRead_only_byte_array((*byte)(unsafe.Pointer(&testData[0])), int64(len(testData)), err)
	defer gsrserde.DeleteRead_only_byte_array(readOnlyArray)

	fmt.Printf("Created read-only byte array with length: %d\n", readOnlyArray.Get_len())

	// Test byte array operations
	fmt.Println("\n=== Testing Byte Array Operations ===")
	arraySize := int64(100)
	mutArray := gsrserde.NewMutable_byte_array(arraySize, err)
	defer gsrserde.DeleteMutable_byte_array(mutArray)

	fmt.Printf("Created mutable byte array with max size: %d\n", mutArray.Get_max_len())

	// Test deserializer functionality
	fmt.Println("\n=== Testing Deserializer Methods ===")
	canDecode := deserializer.Can_decode(readOnlyArray, err)
	fmt.Printf("Can decode test data: %t\n", canDecode)

	fmt.Println("\n=== Go Bindings Example Completed Successfully ===")
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
