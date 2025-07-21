package avro

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/linkedin/goavro/v2"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidAvroData is returned when the data is not valid for AVRO serialization
	ErrInvalidAvroData = fmt.Errorf("data must be compatible with AVRO format")

	// ErrNilData is returned when a nil data is provided
	ErrNilData = fmt.Errorf("avro data cannot be nil")

	// ErrSchemaGeneration is returned when schema generation fails
	ErrSchemaGeneration = fmt.Errorf("failed to generate schema from avro data")

	// ErrSerialization is returned when AVRO serialization fails
	ErrSerialization = fmt.Errorf("avro serialization failed")

	// ErrValidation is returned when AVRO validation fails
	ErrValidation = fmt.Errorf("avro validation failed")

	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("invalid avro schema")
)

// AvroSerializationError represents an error that occurred during AVRO serialization
type AvroSerializationError struct {
	Message string
	Cause   error
}

func (e *AvroSerializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("avro serialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("avro serialization error: %s", e.Message)
}

func (e *AvroSerializationError) Unwrap() error {
	return e.Cause
}

// AvroValidationError represents an error that occurred during AVRO validation
type AvroValidationError struct {
	Message string
	Cause   error
}

func (e *AvroValidationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("avro validation error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("avro validation error: %s", e.Message)
}

func (e *AvroValidationError) Unwrap() error {
	return e.Cause
}

// AvroSerializer handles serialization of AVRO messages.
// It uses the linkedin/goavro library for pure Go implementation.
type AvroSerializer struct {
	// This serializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewAvroSerializer creates a new AVRO serializer instance.
func NewAvroSerializer(config *common.Configuration) *AvroSerializer {
	if config == nil {
		panic("configuration cannot be nil")
	}
	return &AvroSerializer{
		config: config,
	}
}

// Serialize serializes AVRO data to bytes.
// The input data must be compatible with AVRO format (map[string]interface{}, []interface{}, or primitive types).
//
// Parameters:
//
//	data: Must be AVRO-compatible data structure
//
// Returns:
//
//	[]byte: The serialized AVRO data
//	error: Any error that occurred during serialization
func (a *AvroSerializer) Serialize(data interface{}) ([]byte, error) {
	if data == nil {
		return nil, &AvroSerializationError{
			Message: "cannot serialize nil data",
			Cause:   ErrNilData,
		}
	}

	// Validate the data before serialization
	if err := a.ValidateObject(data); err != nil {
		return nil, &AvroSerializationError{
			Message: "data validation failed",
			Cause:   err,
		}
	}

	// Normalize data to ensure compatibility with goavro
	normalizedData, err := a.normalizeData(data)
	if err != nil {
		return nil, &AvroSerializationError{
			Message: "failed to normalize data",
			Cause:   err,
		}
	}

	// Extract schema from original data (not normalized, to maintain correct types)
	schema, err := a.extractSchema(data)
	if err != nil {
		return nil, &AvroSerializationError{
			Message: "failed to extract schema from data",
			Cause:   err,
		}
	}

	// Create codec from schema
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, &AvroSerializationError{
			Message: "failed to create AVRO codec",
			Cause:   err,
		}
	}

	// Serialize using goavro codec with normalized data
	serializedData, err := codec.BinaryFromNative(nil, normalizedData)
	if err != nil {
		return nil, &AvroSerializationError{
			Message: "failed to serialize AVRO data",
			Cause:   err,
		}
	}

	return serializedData, nil
}

// GetSchemaDefinition extracts the schema definition from AVRO data.
// It returns the AVRO schema as a JSON string.
//
// Parameters:
//
//	data: Must be AVRO-compatible data structure
//
// Returns:
//
//	string: The schema definition as JSON
//	error: Any error that occurred during schema extraction
func (a *AvroSerializer) GetSchemaDefinition(data interface{}) (string, error) {
	if data == nil {
		return "", &AvroSerializationError{
			Message: "cannot get schema from nil data",
			Cause:   ErrNilData,
		}
	}

	// Extract schema from data
	schema, err := a.extractSchema(data)
	if err != nil {
		return "", &AvroSerializationError{
			Message: "failed to extract schema from data",
			Cause:   err,
		}
	}

	return schema, nil
}

// Validate validates serialized AVRO data against a schema definition.
// This validates that the data can be deserialized using the provided schema.
//
// Parameters:
//
//	schemaDefinition: The AVRO schema definition as JSON string
//	data: The serialized AVRO data to validate
//
// Returns:
//
//	error: Any validation error, nil if valid
func (a *AvroSerializer) Validate(schemaDefinition string, data []byte) error {
	if data == nil {
		return &AvroValidationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	if len(data) == 0 {
		return &AvroValidationError{
			Message: "data cannot be empty",
			Cause:   ErrValidation,
		}
	}

	if schemaDefinition == "" {
		return &AvroValidationError{
			Message: "schema definition cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Create codec from schema to validate schema format
	codec, err := goavro.NewCodec(schemaDefinition)
	if err != nil {
		return &AvroValidationError{
			Message: "invalid schema definition",
			Cause:   err,
		}
	}

	// Try to deserialize the data to validate it against the schema
	_, _, err = codec.NativeFromBinary(data)
	if err != nil {
		return &AvroValidationError{
			Message: "data does not conform to schema",
			Cause:   err,
		}
	}

	return nil
}

// ValidateObject validates an AVRO data object.
// It checks if the object is compatible with AVRO format.
//
// Parameters:
//
//	data: The AVRO data object to validate
//
// Returns:
//
//	error: Any validation error, nil if valid
func (a *AvroSerializer) ValidateObject(data interface{}) error {
	if data == nil {
		return &AvroValidationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	// Check if data is AVRO-compatible
	if !a.isAvroCompatible(data) {
		return &AvroValidationError{
			Message: fmt.Sprintf("data type %T is not compatible with AVRO format", data),
			Cause:   ErrInvalidAvroData,
		}
	}

	return nil
}

// SetAdditionalSchemaInfo sets additional schema information in the schema object.
// For AVRO, this includes the schema type information.
//
// Parameters:
//
//	data: The AVRO data object
//	schema: The schema object to update
//
// Returns:
//
//	error: Any error that occurred during schema update
func (a *AvroSerializer) SetAdditionalSchemaInfo(data interface{}, schema *gsrserde.Schema) error {
	if data == nil {
		return &AvroSerializationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	if schema == nil {
		return &AvroSerializationError{
			Message: "schema cannot be nil",
			Cause:   fmt.Errorf("schema is nil"),
		}
	}

	// Set the data type as additional info
	schema.AdditionalInfo = reflect.TypeOf(data).String()

	// Ensure DataFormat is set correctly
	if schema.DataFormat == "" {
		schema.DataFormat = "AVRO"
	}

	return nil
}

// extractSchema attempts to extract or infer AVRO schema from data
func (a *AvroSerializer) extractSchema(data interface{}) (string, error) {
	return a.extractSchemaWithContext(data, "GeneratedRecord", make(map[string]bool))
}

// extractSchemaWithContext extracts schema with context to avoid naming conflicts
func (a *AvroSerializer) extractSchemaWithContext(data interface{}, recordName string, usedNames map[string]bool) (string, error) {
	// For now, we'll generate a basic schema based on the data structure
	// In a production environment, you would typically have the schema provided
	// or stored alongside the data
	
	switch v := data.(type) {
	case map[string]interface{}:
		return a.generateRecordSchemaWithContext(v, recordName, usedNames)
	case []interface{}:
		if len(v) == 0 {
			return `{"type": "array", "items": "null"}`, nil
		}
		itemSchema, err := a.extractSchemaWithContext(v[0], recordName+"Item", usedNames)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(`{"type": "array", "items": %s}`, itemSchema), nil
	case string:
		return `"string"`, nil
	case int, int32:
		return `"int"`, nil
	case int64:
		return `"long"`, nil
	case float32:
		return `"float"`, nil
	case float64:
		return `"double"`, nil
	case bool:
		return `"boolean"`, nil
	case []byte:
		return `"bytes"`, nil
	default:
		return "", fmt.Errorf("unsupported data type for schema extraction: %T", data)
	}
}

// generateRecordSchema generates an AVRO record schema from a map
func (a *AvroSerializer) generateRecordSchema(data map[string]interface{}) (string, error) {
	return a.generateRecordSchemaWithContext(data, "GeneratedRecord", make(map[string]bool))
}

// generateRecordSchemaWithContext generates an AVRO record schema from a map with unique naming
func (a *AvroSerializer) generateRecordSchemaWithContext(data map[string]interface{}, recordName string, usedNames map[string]bool) (string, error) {
	// Ensure unique record name
	uniqueRecordName := a.generateUniqueRecordName(recordName, usedNames)
	usedNames[uniqueRecordName] = true
	
	fields := make([]string, 0, len(data))
	
	for key, value := range data {
		var fieldSchema string
		var err error
		
		// For nested records, generate a unique name based on the field name
		if nestedMap, ok := value.(map[string]interface{}); ok {
			// Use capitalized field name for nested record name
			nestedRecordName := strings.ToUpper(key[:1]) + key[1:] + "Record"
			fieldSchema, err = a.generateRecordSchemaWithContext(nestedMap, nestedRecordName, usedNames)
		} else {
			fieldSchema, err = a.extractSchemaWithContext(value, key+"Record", usedNames)
		}
		
		if err != nil {
			return "", fmt.Errorf("failed to extract schema for field %s: %w", key, err)
		}
		field := fmt.Sprintf(`{"name": "%s", "type": %s}`, key, fieldSchema)
		fields = append(fields, field)
	}
	
	schema := fmt.Sprintf(`{
		"type": "record",
		"name": "%s",
		"fields": [%s]
	}`, uniqueRecordName, strings.Join(fields, ", "))
	
	return schema, nil
}

// generateUniqueRecordName generates a unique record name to avoid conflicts
func (a *AvroSerializer) generateUniqueRecordName(baseName string, usedNames map[string]bool) string {
	if !usedNames[baseName] {
		return baseName
	}
	
	// If the base name is taken, append a number
	counter := 1
	for {
		candidateName := fmt.Sprintf("%s%d", baseName, counter)
		if !usedNames[candidateName] {
			return candidateName
		}
		counter++
	}
}

// normalizeData normalizes data to ensure compatibility with goavro
// This ensures that Go types are properly converted to AVRO-compatible formats
func (a *AvroSerializer) normalizeData(data interface{}) (interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Recursively normalize all values in the map
		normalized := make(map[string]interface{})
		for key, value := range v {
			normalizedValue, err := a.normalizeData(value)
			if err != nil {
				return nil, err
			}
			normalized[key] = normalizedValue
		}
		return normalized, nil
		
	case []interface{}:
		// Recursively normalize all elements in the slice
		normalized := make([]interface{}, len(v))
		for i, item := range v {
			normalizedItem, err := a.normalizeData(item)
			if err != nil {
				return nil, err
			}
			normalized[i] = normalizedItem
		}
		return normalized, nil
		
	case bool:
		// Ensure boolean values are properly handled by goavro
		// goavro expects Go bool type, so we just return it as-is
		return v, nil
		
	case int:
		// Convert int to int32 for AVRO compatibility
		return int32(v), nil
		
	case int64:
		// Keep int64 as-is (AVRO long type)
		return v, nil
		
	case int32:
		// Keep int32 as-is (AVRO int type)
		return v, nil
		
	case float32:
		// Keep float32 as-is (AVRO float type)
		return v, nil
		
	case float64:
		// Keep float64 as-is (AVRO double type)
		return v, nil
		
	case string:
		// Keep string as-is
		return v, nil
		
	case []byte:
		// Keep bytes as-is
		return v, nil
		
	case nil:
		// Keep nil as-is
		return nil, nil
		
	default:
		// Handle other slice types by converting to []interface{}
		rv := reflect.ValueOf(data)
		if rv.Kind() == reflect.Slice {
			length := rv.Len()
			normalized := make([]interface{}, length)
			for i := 0; i < length; i++ {
				item := rv.Index(i).Interface()
				normalizedItem, err := a.normalizeData(item)
				if err != nil {
					return nil, err
				}
				normalized[i] = normalizedItem
			}
			return normalized, nil
		}
		
		return nil, fmt.Errorf("unsupported data type for normalization: %T", data)
	}
}

// isAvroCompatible checks if the data type is compatible with AVRO serialization
func (a *AvroSerializer) isAvroCompatible(data interface{}) bool {
	switch data.(type) {
	case map[string]interface{}, []interface{}:
		return true
	case string, int, int32, int64, float32, float64, bool, []byte:
		return true
	case nil:
		return true
	default:
		// Check if it's a slice of compatible types
		v := reflect.ValueOf(data)
		if v.Kind() == reflect.Slice {
			return true // We'll validate the elements during serialization
		}
		return false
	}
}
