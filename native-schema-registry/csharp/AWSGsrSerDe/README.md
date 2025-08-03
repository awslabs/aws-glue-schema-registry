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
dotnet build . --configuration Release
dotnet test .
```

