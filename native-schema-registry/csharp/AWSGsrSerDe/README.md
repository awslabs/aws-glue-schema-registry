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

#### Releasing as a nupkg 
After the build steps are successful, do the following:

```
dotnet pack -c Release --no-build
```

This outputs: ./bin/Release/AWSGsrSerDe.1.1.10.nupkg

#### Running C# tests

```
# Set AWS environment credentials and verify that the 'test-registry' exists in AWS Glue.
# This ensures that libnativeschemaregistry.so can locate its dependent .so files.

export LD_LIBRARY_PATH=/workspaces/aws-glue-schema-registry/native-schema-registry/csharp/AWSGsrSerDe/AWSGsrSerDe/bin/Release/net8.0

# Run the test suite
dotnet test .
```

#### Computing test coverage
Run the following commands 

```
dotnet tool install -g dotnet-reportgenerator-globaltool
dotnet test AWSGsrSerDe.Tests.csproj --collect:"XPlat Code Coverage"
reportgenerator -reports:**/coverage.cobertura.xml -targetdir:coverage-report -reporttypes:Html
```

This produces the coverage report in AWSGsrSerDe/AWSGsrSerDe.Tests/coverage-report/index.html

### Using Csharp Glue Schema client library with KafkaFlow for SerDes
__Sample serializer usage:__

```csharp
services.AddKafka(kafka => kafka
    .UseConsoleLog()
    .AddCluster(cluster => cluster
        .WithBrokers(new[] { "localhost:9092" })
        .AddProducer<CustomerProducer>(producer => producer
            .DefaultTopic("customer-events")
            .AddMiddlewares(m => m
                .AddSerializer<GlueSchemaRegistryKafkaFlowProtobufSerializer<Customer>>(
                    () => new GlueSchemaRegistryKafkaFlowProtobufSerializer<Customer>("config/gsr-config.properties")
                )
            )
        )
    )
);
```

__Sample deserializer usage:__

```csharp
.AddConsumer(consumer => consumer
    .Topic("customer-events")
    .WithGroupId("customer-group")
    .WithBufferSize(100)
    .WithWorkersCount(10)
    .AddMiddlewares(middlewares => middlewares
        .AddDeserializer<GlueSchemaRegistryKafkaFlowProtobufDeserializer<Customer>>(
            () => new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Customer>("config/gsr-config.properties")
        )
        .AddTypedHandlers(h => h.AddHandler<CustomerHandler>())
    )
)
```

### Using Csharp Glue Schema client library for Kafka SerDes
__Sample serializer usage:__

```csharp
private static readonly string CONFIG_PATH = "<PATH_TO_CONFIG_FILE>";
var protobufSerializer = new GlueSchemaRegistryKafkaSerializer(CONFIG_PATH);
var serialized = protobufSerializer.Serialize(message, message.Descriptor.FullName, "PROTOBUF");
// send serialized bytes to Kafka using producer.Produce(serialized)
```

__Sample deserializer usage:__

```csharp
private static readonly string CONFIG_PATH = "<PATH_TO_CONFIG_FILE>";
var dataConfig = new GlueSchemaRegistryDataFormatConfiguration(
    new Dictionary<string, dynamic>
    {
        { 
            GlueSchemaRegistryConstants.ProtobufMessageDescriptor, message.Descriptor 
        }
    }
);
var protobufDeserializer = new GlueSchemaRegistryKafkaDeserializer(CONFIG_PATH, dataConfig);

// read message from Kafka using serialized = consumer.Consume()
var deserializedObject = protobufDeserializer.Deserialize(message.Descriptor.FullName, serialized);
```