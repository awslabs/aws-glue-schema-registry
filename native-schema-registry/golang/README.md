# AWS Glue Schema Registry - Go Bindings

This directory contains the Go language bindings for the AWS Glue Schema Registry, providing a Go interface to the high-performance C library.

## Features

- **High Performance**: Direct bindings to optimized C library
- **Complete API Coverage**: Access to all serialization/deserialization functions
- **Memory Safe**: Proper resource management with destructors
- **Go Idiomatic**: Interface-based design following Go conventions
- **Automatic Build**: Integrated with CMake build system

## Project Structure

```
golang/
├── pkg/gsrserde/          # Core Go package with SWIG bindings
│   ├── GsrSerDe.go       # Generated Go bindings (SWIG)
│   ├── cgo_flags.go      # CGO compilation flags
│   └── *.c               # Generated C wrapper code
├── lib/                   # Shared libraries
│   ├── libaws_common_memalloc.so
│   ├── libnative_schema_registry_c_data_types.so
│   ├── libnative_schema_registry_c.so
│   └── libnativeschemaregistry.so
├── examples/             # Usage examples
│   └── basic/main.go    # Basic usage example
├── test/                 # Test files
├── go.mod               # Go module definition
└── README.md           # This file
```

## Building

The Go bindings are automatically generated and built as part of the main CMake build process:

```bash
# From the native-schema-registry/c directory
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

This will:
1. Generate Go bindings using SWIG
2. Compile the C wrapper code
3. Copy shared libraries to `lib/` directory
4. Place generated Go code in `pkg/gsrserde/`

## Usage

### Basic Example

```go
package main

import (
    "fmt"
    "unsafe"
    "github.com/awslabs/aws-glue-schema-registry/golang/pkg/gsrserde"
)

func main() {
    // Create schema
    var err gsrserde.Glue_schema_registry_error
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

    // Use the bindings...
}
```

### Running Examples

```bash
cd examples/basic
go run main.go
```

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

The bindings use SWIG-generated wrappers that properly manage C memory:

- **Constructors**: `New*()` functions allocate resources
- **Destructors**: `Delete*()` functions clean up resources  
- **RAII Pattern**: Use `defer` to ensure cleanup

```go
schema := gsrserde.NewGlue_schema_registry_schema(...)
defer gsrserde.DeleteGlue_schema_registry_schema(schema) // Always cleanup
```

## Integration with AWS

These bindings provide the core serialization/deserialization functionality. For full AWS integration:

1. **AWS SDK**: Use AWS SDK for Go to interact with Glue Schema Registry service
2. **Schema Management**: Register/retrieve schemas via AWS APIs
3. **Configuration**: Set up AWS credentials and region
4. **Production Usage**: Combine with Kafka, Kinesis, or other data streaming services

## Performance Notes

- **Zero-Copy Operations**: Direct memory access where possible
- **Minimal Allocations**: Efficient memory usage patterns
- **C Library Speed**: Leverages optimized C implementation
- **Concurrent Safe**: Multiple goroutines can use separate instances

## Development

### Prerequisites
- Go 1.20+
- SWIG 4.0+
- CMake 3.10+
- GCC/Clang compiler

### Building from Source
```bash
# Ensure SWIG and CMake are installed
cd native-schema-registry/c
mkdir build && cd build
cmake ..
make GsrSerDeGoGen  # Build just Go bindings
```

### Testing
```bash
cd golang
go test ./...
```

## Troubleshooting

### Common Issues

1. **Missing Shared Libraries**
   ```
   error while loading shared libraries: libnativeschemaregistry.so
   ```
   Solution: Ensure `lib/` directory is in your `LD_LIBRARY_PATH`

2. **CGO Compilation Errors**
   ```
   fatal error: 'glue_schema_registry_serde.h' file not found
   ```
   Solution: Run CMake build to generate headers

3. **Import Path Issues**
   ```
   cannot find package "github.com/awslabs/aws-glue-schema-registry/golang/pkg/gsrserde"
   ```
   Solution: Use `go mod tidy` and check module path

### Debug Mode

Build with debug symbols:
```bash
cmake .. -DCMAKE_BUILD_TYPE=Debug
```

## Contributing

1. Modify the SWIG interface file: `../c/src/swig/glue_schema_registry_serde.i`
2. Rebuild using CMake to regenerate bindings
3. Test with the example applications
4. Submit pull request with tests

## License

See the main project LICENSE file for details.
