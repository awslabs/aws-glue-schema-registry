package deserializer

import (
	gsrserde "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go"
)

// DataFormatDeserializer defines the interface for format-specific deserializers.
// Implementations of this interface handle deserialization for specific data formats
// such as Avro, Protobuf, and JSON.
type DataFormatDeserializer interface {
	// Deserialize takes encoded data and a schema, and returns the deserialized object.
	// The actual type of the returned object depends on the data format:
	// - Avro: typically returns a map[string]interface{} or specific Avro record type
	// - Protobuf: returns a proto.Message implementation
	// - JSON: returns map[string]interface{} or custom struct
	//
	// Parameters:
	//   data: The encoded byte data to deserialize
	//   schema: The schema information needed for deserialization
	//
	// Returns:
	//   interface{}: The deserialized object (type depends on format)
	//   error: Any error that occurred during deserialization
	Deserialize(data []byte, schema *gsrserde.Schema) (interface{}, error)

}
