# AWS Glue Schema Registry - Go Module

This is a standalone Go module for the AWS Glue Schema Registry, providing high-performance serialization and deserialization capabilities through native bindings.

## Features

- **High Performance**: Direct bindings to optimized native libraries
- **Complete API Coverage**: Access to all serialization/deserialization functions
- **Memory Safe**: Proper resource management with destructors
- **Go Idiomatic**: Interface-based design following Go conventions
- **Self-Contained**: All required libraries and headers included

## Installation

This module is distributed as platform-specific releases. Download the appropriate release for your platform:

- `aws-glue-schema-registry-go-linux-amd64.tar.gz` - Linux on Intel/AMD 64-bit
- `aws-glue-schema-registry-go-linux-arm64.tar.gz` - Linux on ARM 64-bit
- `aws-glue-schema-registry-go-darwin-amd64.tar.gz` - macOS on Intel
- `aws-glue-schema-registry-go-darwin-arm64.tar.gz` - macOS on Apple Silicon
- `aws-glue-schema-registry-go-windows-amd64.tar.gz` - Windows 64-bit

Extract the archive and use as a local Go module:

```bash
# Extract the release
tar -xzf aws-glue-schema-registry-go-linux-amd64.tar.gz

# Use in your project
cd your-project
go mod edit -replace github.com/awslabs/aws-glue-schema-registry-go=./path/to/golang
go mod tidy
```

## Project Structure

```
golang/
├── include/          # C headers needed for compilation
├── lib/             # Native libraries (platform-specific)
├── pkg/gsrserde/    # Core Go package with native bindings
│   ├── GsrSerDe.go       # Generated Go bindings
│   ├── cgo_flags.go      # CGO compilation flags
│   └── *.c               # Generated C wrapper code
├── examples/        # Usage examples
│   └── basic/main.go    # Basic usage example
├── go.mod          # Go module definition
└── README.md       # This file
```

## Quick Start

```go
package main

import (
    "fmt"
    "unsafe"
    "github.com/awslabs/aws-glue-schema-registry-go/pkg/gsrserde"
)

func main() {
    // Create error handler
    var err gsrserde.Glue_schema_registry_error
    
    // Create schema
    schema := gsrserde.NewGlue_schema_registry_schema(
        "user-schema",
        `{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}]}`,
        "AVRO",
        "",
        err,
    )
    defer gsrserde.DeleteGlue_schema_registry_schema(schema)

    // Create serializer
    serializer := gsrserde.NewGlue_schema_registry_serializer(err)
    defer gsrserde.DeleteGlue_schema_registry_serializer(serializer)

    // Create deserializer
    deserializer := gsrserde.NewGlue_schema_registry_deserializer(err)
    defer gsrserde.DeleteGlue_schema_registry_deserializer(deserializer)

    fmt.Println("AWS Glue Schema Registry Go module initialized successfully")
}
```

## Building

This module comes pre-built with all necessary libraries and headers. Simply run:

```bash
go build ./examples/basic
```

No additional build tools (CMake, Java, etc.) are required.

## Core Types

### Schema Management
- `Glue_schema_registry_schema` - Schema definition with metadata
  - `Get_schema_name()` - Retrieve schema name
  - `Get_schema_def()` - Get schema definition
  - `Get_data_format()` - Get data format (AVRO, JSON, etc.)

### Serialization
- `Glue_schema_registry_serializer` - High-performance serializer
  - `Encode()` - Serialize data with schema

### Deserialization  
- `Glue_schema_registry_deserializer` - High-performance deserializer
  - `Decode()` - Deserialize data
  - `Can_decode()` - Check if data can be decoded
  - `Decode_schema()` - Extract schema from encoded data

### Data Handling
- `Read_only_byte_array` - Immutable byte array for input data
- `Mutable_byte_array` - Mutable byte array for output data

## Memory Management

The bindings use proper resource management:

- **Constructors**: `New*()` functions allocate resources
- **Destructors**: `Delete*()` functions clean up resources  
- **RAII Pattern**: Use `defer` to ensure cleanup

```go
schema := gsrserde.NewGlue_schema_registry_schema(...)
defer gsrserde.DeleteGlue_schema_registry_schema(schema) // Always cleanup
```

## Requirements

- Go 1.20+
- CGO enabled (`CGO_ENABLED=1`)
- Platform-specific native libraries (included in release)


## Troubleshooting

### Common Issues

1. **CGO Not Enabled**
   ```
   # cgo: C compiler not found
   ```
   Solution: Install a C compiler and ensure `CGO_ENABLED=1`

2. **Missing Libraries**
   ```
   error while loading shared libraries: libnativeschemaregistry.so
   ```
   Solution: Ensure you're using the correct platform-specific release

3. **Import Path Issues**
   ```
   cannot find package
   ```
   Solution: Use `go mod edit -replace` to point to the local module path

## Contributing

This module is generated from the main AWS Glue Schema Registry project. To contribute:

1. Submit changes to the main repository
2. Libraries and bindings are automatically generated during the build process
3. Platform-specific releases are created via CI/CD

## License

See the main project LICENSE file for details.
