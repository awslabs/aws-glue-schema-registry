package interfaces

import (
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/GsrSerDe"
)

// SwigOperations defines the interface for all SWIG-generated operations
// This allows us to inject mock implementations during testing to avoid AWS API calls
type SwigOperations interface {
	// Serializer operations
	NewSerializer(err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_serializer
	DeleteSerializer(serializer GsrSerDe.Glue_schema_registry_serializer)
	SerializerEncode(serializer GsrSerDe.Glue_schema_registry_serializer, data GsrSerDe.Read_only_byte_array, transportName string, schema GsrSerDe.Glue_schema_registry_schema, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Mutable_byte_array

	// Deserializer operations
	NewDeserializer(err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_deserializer
	DeleteDeserializer(deserializer GsrSerDe.Glue_schema_registry_deserializer)
	DeserializerDecode(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Mutable_byte_array
	DeserializerCanDecode(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) bool
	DeserializerDecodeSchema(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_schema

	// Schema operations
	NewSchema(name, definition, dataFormat, additionalInfo string, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_schema
	DeleteSchema(schema GsrSerDe.Glue_schema_registry_schema)

	// Memory operations
	NewReadOnlyByteArray(dataPtr *byte, length int64, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Read_only_byte_array
	DeleteReadOnlyByteArray(roba GsrSerDe.Read_only_byte_array)
	DeleteMutableByteArray(mba GsrSerDe.Mutable_byte_array)

	// Error operations
	CreateErrorHolder() GsrSerDe.Glue_schema_registry_error
}

// ProductionSwigOperations implements SwigOperations using actual SWIG-generated functions
// This is the production implementation that calls the real AWS APIs
type ProductionSwigOperations struct{}

// NewProductionSwigOperations creates a new production SWIG operations instance
func NewProductionSwigOperations() *ProductionSwigOperations {
	return &ProductionSwigOperations{}
}

// Serializer operations
func (p *ProductionSwigOperations) NewSerializer(err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_serializer {
	return GsrSerDe.NewGlue_schema_registry_serializer(err)
}

func (p *ProductionSwigOperations) DeleteSerializer(serializer GsrSerDe.Glue_schema_registry_serializer) {
	GsrSerDe.DeleteGlue_schema_registry_serializer(serializer)
}

func (p *ProductionSwigOperations) SerializerEncode(serializer GsrSerDe.Glue_schema_registry_serializer, data GsrSerDe.Read_only_byte_array, transportName string, schema GsrSerDe.Glue_schema_registry_schema, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Mutable_byte_array {
	return serializer.Encode(data, transportName, schema, err)
}

// Deserializer operations
func (p *ProductionSwigOperations) NewDeserializer(err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_deserializer {
	return GsrSerDe.NewGlue_schema_registry_deserializer(err)
}

func (p *ProductionSwigOperations) DeleteDeserializer(deserializer GsrSerDe.Glue_schema_registry_deserializer) {
	GsrSerDe.DeleteGlue_schema_registry_deserializer(deserializer)
}

func (p *ProductionSwigOperations) DeserializerDecode(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Mutable_byte_array {
	return deserializer.Decode(data, err)
}

func (p *ProductionSwigOperations) DeserializerCanDecode(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) bool {
	return deserializer.Can_decode(data, err)
}

func (p *ProductionSwigOperations) DeserializerDecodeSchema(deserializer GsrSerDe.Glue_schema_registry_deserializer, data GsrSerDe.Read_only_byte_array, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_schema {
	return deserializer.Decode_schema(data, err)
}

// Schema operations
func (p *ProductionSwigOperations) NewSchema(name, definition, dataFormat, additionalInfo string, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Glue_schema_registry_schema {
	return GsrSerDe.NewGlue_schema_registry_schema(name, definition, dataFormat, additionalInfo, err)
}

func (p *ProductionSwigOperations) DeleteSchema(schema GsrSerDe.Glue_schema_registry_schema) {
	GsrSerDe.DeleteGlue_schema_registry_schema(schema)
}

// Memory operations
func (p *ProductionSwigOperations) NewReadOnlyByteArray(dataPtr *byte, length int64, err GsrSerDe.Glue_schema_registry_error) GsrSerDe.Read_only_byte_array {
	return GsrSerDe.NewRead_only_byte_array(dataPtr, length, err)
}

func (p *ProductionSwigOperations) DeleteReadOnlyByteArray(roba GsrSerDe.Read_only_byte_array) {
	GsrSerDe.DeleteRead_only_byte_array(roba)
}

func (p *ProductionSwigOperations) DeleteMutableByteArray(mba GsrSerDe.Mutable_byte_array) {
	GsrSerDe.DeleteMutable_byte_array(mba)
}

// Error operations
func (p *ProductionSwigOperations) CreateErrorHolder() GsrSerDe.Glue_schema_registry_error {
	// In the SWIG interface, we just pass nil for the error parameter
	// The C code will allocate error if needed
	return nil
}
