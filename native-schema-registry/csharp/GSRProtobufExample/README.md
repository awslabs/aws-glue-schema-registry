# GSR Protobuf KafkaFlow Example

This example demonstrates how to integrate AWS Glue Schema Registry (GSR) with KafkaFlow for C# applications using Protocol Buffers (Protobuf) serialization.

## Overview

This example consists of:
- **Producer Application**: Publishes different types of protobuf messages to Kafka topics using GSR serialization
- **Consumer Application**: Consumes messages from Kafka topics using GSR deserialization
- **Shared Library**: Contains protobuf message definitions and common utilities

## Architecture

```
┌─────────────────┐    ┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Producer      │───▶│    Kafka    │───▶│  AWS Glue        │───▶│   Consumer      │
│   Application   │    │   Cluster   │    │  Schema Registry │    │   Application   │
└─────────────────┘    └─────────────┘    └──────────────────┘    └─────────────────┘
```

## Project Structure

```
GSRProtobufExample/
├── src/
│   ├── GSRProtobufExample.Shared/         # Shared protobuf definitions
│   │   └── Protos/                        # Protocol buffer schema files
│   │       ├── user.proto
│   │       ├── product.proto
│   │       ├── order.proto
│   │       ├── event.proto
│   │       └── company.proto
│   ├── GSRProtobufExample.Serializer/     # Producer application
│   │   ├── Program.cs
│   │   └── Services/
│   │       ├── GsrProtobufSerializer.cs
│   │       └── MessagePublisher.cs
│   └── GSRProtobufExample.Deserializer/   # Consumer application
│       ├── Program.cs
│       └── Services/
│           ├── GsrProtobufDeserializer.cs
│           └── MessageHandlers.cs
├── config.properties                      # GSR configuration
├── docker-compose.yml                     # Kafka infrastructure
└── README.md
```

## Key Features

### AWS Glue Schema Registry Integration
- **Schema Management**: Automatic schema registration and versioning
- **Compression**: Configurable compression (GZIP/None)
- **Caching**: Schema caching for improved performance
- **Evolution**: Backward compatible schema evolution

### KafkaFlow Integration
- **Modern C# API**: Fluent configuration and dependency injection
- **Custom Serializers**: GSR-aware serialization/deserialization
- **Message Routing**: Topic-based message routing with handlers
- **Logging**: Structured logging with configurable levels

### Protocol Buffers Support
- **Multiple Message Types**: User, Product, Order, Event, Company schemas
- **Type Safety**: Strongly-typed message handling
- **Code Generation**: Automatic C# class generation from .proto files

## Prerequisites

1. **.NET 8.0 SDK** or later
2. **Docker and Docker Compose** for Kafka infrastructure
3. **AWS CLI** configured with appropriate credentials
4. **AWS Glue Schema Registry** setup in your AWS account

## Configuration

### GSR Configuration (`config.properties`)

```properties
# AWS Region
region=us-east-1

# Schema Registry Name
schemaregistryname=gsr-protobuf-example

# Data Format
dataFormat=PROTOBUF

# Compression Type (NONE, GZIP)
compressiontype=NONE

# Cache Settings
cachettl=86400000
cachesize=200

# Schema Evolution Settings
compatibilitysetting=BACKWARD

# Optional: Schema Auto-registration
autoregisterschemas=true
```

### AWS Credentials

Configure AWS credentials using one of these methods:

1. **AWS CLI**: `aws configure`
2. **Environment Variables**:
   ```bash
   export AWS_ACCESS_KEY_ID=your-access-key
   export AWS_SECRET_ACCESS_KEY=your-secret-key
   export AWS_DEFAULT_REGION=us-east-1
   ```
3. **IAM Roles** (recommended for EC2/ECS deployments)

## Running the Example

### 1. Start Kafka Infrastructure

```bash
# Start Kafka cluster
docker-compose up -d

# Wait for services to be ready (30-60 seconds)
docker-compose logs -f kafka
```

### 2. Build the Applications

```bash
# Build all projects
dotnet build

# Or build individual projects
dotnet build src/GSRProtobufExample.Serializer/
dotnet build src/GSRProtobufExample.Deserializer/
```

### 3. Run Consumer (in separate terminal)

```bash
dotnet run --project src/GSRProtobufExample.Deserializer/
```

### 4. Run Producer (in separate terminal)

```bash
dotnet run --project src/GSRProtobufExample.Serializer/
```

## Message Flow

### Producer Workflow
1. Creates various protobuf message instances
2. Publishes messages to different Kafka topics:
   - `user-events` → User messages
   - `product-events` → Product messages  
   - `order-events` → Order messages
   - `system-events` → Event messages
   - `company-events` → Company messages

### Consumer Workflow
1. Subscribes to all configured topics
2. Receives serialized messages from Kafka
3. Deserializes using GSR with schema validation
4. Routes messages to appropriate handlers based on type
5. Processes and logs message content

## GSR Integration Details

### Serialization Process
1. **Message Creation**: Create protobuf message instance
2. **Schema Registration**: Register/retrieve schema from GSR
3. **Serialization**: Convert message to bytes with GSR header
4. **Publication**: Send to Kafka topic

### Deserialization Process
1. **Message Reception**: Receive bytes from Kafka
2. **Header Parsing**: Extract schema information from GSR header
3. **Schema Lookup**: Retrieve schema from GSR (with caching)
4. **Deserialization**: Convert bytes to strongly-typed message
5. **Processing**: Route to appropriate message handler

## Monitoring and Troubleshooting

### Logs
Both applications provide structured logging:
- **KafkaFlow events**: Connection, consumption, production status
- **GSR operations**: Schema registration, caching, errors
- **Message processing**: Handler execution, errors, performance

### Common Issues

1. **Schema Registry Connection**
   - Verify AWS credentials and region
   - Check GSR service availability
   - Validate IAM permissions

2. **Kafka Connection**
   - Ensure Kafka is running (`docker-compose ps`)
   - Check broker connectivity
   - Verify topic creation

3. **Schema Evolution Errors**
   - Review compatibility settings
   - Check schema changes for backward compatibility
   - Monitor GSR schema versions

### Performance Tuning

1. **Schema Caching**
   - Adjust `cachesize` and `cachettl` values
   - Monitor cache hit rates

2. **Compression**
   - Enable GZIP for large messages
   - Test performance impact

3. **Batch Processing**
   - Configure KafkaFlow batch settings
   - Adjust consumer group settings

## Extending the Example

### Adding New Message Types
1. Create new `.proto` file in `Shared/Protos/`
2. Add message handler in consumer
3. Update publisher to send new message type
4. Register new topic routing

### Custom Serialization Logic
- Extend `GsrProtobufSerializer`/`GsrProtobufDeserializer`
- Add message preprocessing/postprocessing
- Implement custom error handling

### Integration Testing
- Use testcontainers for integration tests
- Mock GSR responses for unit tests
- Validate schema evolution scenarios

## Resources

- [KafkaFlow Documentation](https://farfetch.github.io/kafkaflow/)
- [AWS Glue Schema Registry Documentation](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
- [Protocol Buffers Documentation](https://developers.google.com/protocol-buffers)

## License

This example is provided under the same license as the AWS Glue Schema Registry project.
