# GSR Protobuf Example with KafkaFlow

This example demonstrates how to use AWS Glue Schema Registry (GSR) with Protocol Buffers in a C# application using KafkaFlow. It showcases serialization and deserialization of protobuf messages through Kafka topics with GSR schema management.

## Overview

The example consists of three main projects:

1. **GSRProtobufExample.Shared** - Contains protobuf schema definitions (`.proto` files) and generated C# classes
2. **GSRProtobufExample.Serializer** - Producer application that publishes messages to Kafka using GSR
3. **GSRProtobufExample.Deserializer** - Consumer application that consumes messages from Kafka using GSR

## Features Demonstrated

- **Schema Registry Integration**: Uses AWS Glue Schema Registry for schema management
- **Protocol Buffers**: Demonstrates various protobuf features:
  - Simple messages (User)
  - Nested messages (Product with Price)
  - Repeated fields (Order with OrderItems)
  - Enums (Event with EventType)
  - Complex messages with imports, maps, and oneof (Company)
- **KafkaFlow Framework**: Modern .NET Kafka framework with middleware support
- **Automatic Serialization/Deserialization**: Transparent protobuf serialization with GSR header injection
- **Schema Evolution**: Supports schema evolution through AWS Glue Schema Registry

## Protobuf Schema Definitions

### 1. User (user.proto)
Simple message with basic fields demonstrating fundamental protobuf structure.

### 2. Product (product.proto) 
Nested message structure with embedded Price message.

### 3. Order (order.proto)
Demonstrates repeated fields with OrderItem collection.

### 4. Event (event.proto)
Shows enum usage with EventType enumeration.

### 5. Company (company.proto)
Complex message demonstrating:
- Message imports (User)
- Map fields (metadata)
- Oneof fields (contact_method)
- Nested Address message

## Prerequisites

- .NET 8.0 or later
- Docker and Docker Compose (for local Kafka)
- AWS CLI configured with appropriate permissions for Glue Schema Registry
- Access to AWS Glue Schema Registry

## AWS Configuration

The example assumes you have AWS credentials configured through one of the following methods:
- AWS CLI (`aws configure`)
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- IAM roles (if running on EC2)
- AWS profiles

Required AWS permissions for Glue Schema Registry:
- `glue:GetSchema`
- `glue:GetSchemaByDefinition`
- `glue:RegisterSchemaVersion`
- `glue:PutSchemaVersionMetadata`
- `glue:GetSchemaVersionsDiff`

## Running the Example

### 1. Start Kafka Infrastructure

```bash
# Start Kafka and Zookeeper using Docker Compose
docker-compose up -d

# Wait for services to be ready (about 30 seconds)
docker-compose logs -f kafka
```

### 2. Build the Solution

```bash
# Navigate to the project directory
cd aws-glue-schema-registry/native-schema-registry/csharp/GSRProtobufExample

# Restore dependencies and build
dotnet build
```

### 3. Run the Deserializer (Consumer)

```bash
# Start the consumer first to create consumer groups
dotnet run --project src/GSRProtobufExample.Deserializer
```

### 4. Run the Serializer (Producer)

In a separate terminal:

```bash
# Start the producer to publish sample messages
dotnet run --project src/GSRProtobufExample.Serializer
```

## Configuration

Both applications are configured to use:
- **Schema Registry Name**: `gsr-protobuf-example`
- **AWS Region**: `us-east-1`
- **Data Format**: `PROTOBUF`
- **Compression**: `NONE`
- **Cache TTL**: 24 hours (86400000 ms)
- **Cache Size**: 200 schemas

### Topics and Consumer Groups

| Topic | Consumer Group | Message Type |
|-------|----------------|--------------|
| `users` | `gsr-protobuf-example-users` | User |
| `products` | `gsr-protobuf-example-products` | Product |
| `orders` | `gsr-protobuf-example-orders` | Order |
| `events` | `gsr-protobuf-example-events` | Event |
| `companies` | `gsr-protobuf-example-companies` | Company |

## Message Flow

1. **Producer (Serializer)**:
   - Creates sample protobuf messages
   - Serializes messages to byte arrays
   - Registers/validates schemas with GSR
   - Publishes messages to appropriate Kafka topics with GSR headers

2. **Consumer (Deserializer)**:
   - Consumes messages from Kafka topics
   - Extracts GSR headers to identify schema
   - Deserializes protobuf messages using schema information
   - Processes and logs message content

## Schema Registry Integration

The example automatically handles:
- **Schema Registration**: First-time schema registration with GSR
- **Schema Validation**: Ensuring message compatibility with registered schemas
- **Schema Caching**: Local caching of schemas for performance
- **Schema Evolution**: Support for compatible schema changes

## Troubleshooting

### Common Issues

1. **AWS Credentials Not Configured**
   ```
   Error: Unable to load AWS credentials
   Solution: Configure AWS CLI or set environment variables
   ```

2. **Schema Registry Access Denied**
   ```
   Error: Access denied to Glue Schema Registry
   Solution: Verify IAM permissions for Glue operations
   ```

3. **Kafka Connection Refused**
   ```
   Error: Connection refused to localhost:9092
   Solution: Ensure Kafka is running via docker-compose up
   ```

4. **Schema Compatibility Issues**
   ```
   Error: Schema evolution validation failed
   Solution: Review protobuf schema changes for compatibility
   ```

### Logs and Debugging

Enable verbose logging by modifying `appsettings.json` or environment variables:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "KafkaFlow": "Debug",
      "Amazon.GlueSchemaRegistry": "Debug"
    }
  }
}
```

## Cleanup

Stop and remove Docker containers:

```bash
docker-compose down -v
```

## Architecture Benefits

1. **Schema Governance**: Centralized schema management through AWS GSR
2. **Backward Compatibility**: Support for schema evolution
3. **Performance**: Schema caching reduces registry calls
4. **Type Safety**: Strong typing through generated protobuf classes
5. **Monitoring**: Integration with AWS CloudWatch for GSR metrics

## Next Steps

- Implement schema evolution scenarios
- Add monitoring and alerting
- Integrate with CI/CD pipelines for schema validation
- Add custom serialization configurations
- Implement error handling and retry policies

## Recent Updates

This example has been updated to be compatible with KafkaFlow 3.0.7 and fix compilation issues:

- **KafkaFlow 3.0.7 Compatibility**: Updated serializer and deserializer interfaces to match the new KafkaFlow API
  - `ISerializer.SerializeAsync(object message, Stream output, ISerializerContext context)`
  - `IDeserializer.DeserializeAsync(Stream input, Type type, ISerializerContext context)`
- **Package Dependencies**: Removed deprecated `KafkaFlow.Microsoft.Extensions.Logging` package
- **Build System**: Fixed project references and ensured all projects build successfully
- **Interface Implementations**: Updated custom GSR serializers to implement the correct async interfaces

The example now builds and runs successfully with the latest versions of all dependencies.

## References

- [AWS Glue Schema Registry Documentation](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
- [KafkaFlow Documentation](https://farfetch.github.io/kafkaflow/)
- [Protocol Buffers Documentation](https://developers.google.com/protocol-buffers)
- [Native Schema Registry C# Implementation](../../../README.md)
