// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package gsrserde provides Go bindings for AWS Glue Schema Registry serialization and deserialization.
//
// This package offers a Go interface to the AWS Glue Schema Registry, allowing you to:
//   - Serialize data with schema registry headers
//   - Deserialize data encoded with schema registry headers
//   - Work with multiple data formats (Avro, JSON, Protobuf)
//   - Integrate with Kafka for schema-aware message processing
//
// Basic usage:
//
//	import "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang"
//
//	// Create a schema
//	schema := gsrserde.NewSchema("MySchema", schemaDefinition, "AVRO")
//
//	// Create a serializer
//	serializer, err := gsrserde.NewSerializer()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer serializer.Close()
//
//	// Encode data
//	encoded, err := serializer.Encode("transport-name", schema, data)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create a deserializer
//	deserializer, err := gsrserde.NewDeserializer()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer deserializer.Close()
//
//	// Decode data
//	decoded, err := deserializer.Decode(encoded)
//	if err != nil {
//		log.Fatal(err)
//	}
package gsrserde
