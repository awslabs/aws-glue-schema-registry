# AWS Glue Schema Registry - Go Module

This is a standalone Go module for the AWS Glue Schema Registry, providing high-performance serialization and deserialization capabilities through native bindings.

## Installation

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

├── lib/             # Native libraries (platform-specific)
    ├── include/          # C headers needed for compilation
├── pkg/gsrserde-go/    # Core Go package with native bindings
│   ├── cgo_flags.go      # CGO compilation flags
├── go.mod          # Go module definition
└── README.md       # This file
```

## Quick Start
Example usage can be found in the GolangDemoGSRKafka module for both serializer and deserializer.

GCC is needed in coordination with CGO in order to compile the relevant C libraries.


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
