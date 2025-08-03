# AWS Glue Schema Registry Serializers / De-serializers for C#

This package provides the Glue Schema Registry (GSR) serializers / de-serializers for Avro, JSON and Protobuf data formats.


## Development
The C# serializers / de-serializers (SerDes) are built as bindings over existing C library of GSR SerDes. The C library is a facade over GraalVM compiled Java SerDes.

### Building
#### Building the C / Java code
Follow the instructions in those specific projects to build them.

#### Building C# code 

```
dotnet clean .
# For Debug configuration
dotnet build .
# For Release configuration
dotnet build . --configuration Release
```

### Running C# tests

```
# Set AWS environment credentials and verify that the 'test-registry' exists in AWS Glue.
# This ensures that libnativeschemaregistry.so can locate its dependent .so files.

export LD_LIBRARY_PATH=/workspaces/aws-glue-schema-registry/native-schema-registry/csharp/AWSGsrSerDe/AWSGsrSerDe/bin/Release/net8.0

# Run the test suite
dotnet test .
```

