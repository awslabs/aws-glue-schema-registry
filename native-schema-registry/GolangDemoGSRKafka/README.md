# Golang Demo with AWS Glue Schema Registry and Kafka

This demo showcases how to use AWS Glue Schema Registry (GSR) with Protocol Buffers and Kafka in Go, using the kafka-go library.

## Features

- **Protobuf Schema Generation**: Generate Go files from .proto definitions
- **GSR Integration**: Serialization and deserialization using AWS Glue Schema Registry
- **Kafka Integration**: Producer and consumer using kafka-go library  
- **Command Line Interface**: Configurable via command line arguments
- **Multi-topic Support**: Handle both User and Order message types

## Project Structure

```
├── cmd/
│   ├── producer/        # Producer application
│   └── consumer/        # Consumer application
├── pkg/
│   ├── config/          # Configuration management
│   └── proto/           # Generated protobuf files
├── protos/              # Protobuf schema definitions
├── data/                # Sample JSON data files
└── go.mod
```

## Prerequisites

- Go 1.20+
- Protocol Buffers compiler (`protoc`)
- Kafka running locally or accessible remotely
- AWS credentials configured (for GSR)
- AWS Glue Schema Registry set up

## Quick Start

### 1. Generate Protobuf Files

```bash
go generate ./pkg/proto
```

### 2. Run the Producer

```bash
# Using defaults (localhost:9092, user-events, order-events topics)
go run cmd/producer/main.go

# Custom configuration
go run cmd/producer/main.go \
  -brokers=localhost:9092 \
  -user-topic=users \
  -order-topic=orders \
  -aws-region=us-west-2
```

### 3. Run the Consumer

```bash
# Using defaults
go run cmd/consumer/main.go

# Custom configuration
go run cmd/consumer/main.go \
  -brokers=localhost:9092 \
  -topics=users,orders \
  -group-id=my-consumer-group \
  -aws-region=us-west-2
```

## Command Line Options

### Producer

- `-brokers`: Kafka brokers (comma-separated) - default: `localhost:9092`
- `-user-topic`: Topic for user messages - default: `user-events`
- `-order-topic`: Topic for order messages - default: `order-events`
- `-aws-region`: AWS region for GSR - default: `us-east-1`

### Consumer

- `-brokers`: Kafka brokers (comma-separated) - default: `localhost:9092`
- `-user-topic`: Topic for user messages - default: `user-events`
- `-order-topic`: Topic for order messages - default: `order-events`
- `-topics`: Topics to consume (comma-separated) - default: `user-events,order-events`
- `-group-id`: Consumer group ID - default: `gsr-demo-consumer`
- `-aws-region`: AWS region for GSR - default: `us-east-1`

## How It Works

### Producer

1. **Protobuf Configuration**: Creates GSR configuration with message descriptors using protobuf reflection
2. **GSR Serialization**: Uses `serializer.NewSerializer()` following the integration test pattern
3. **Data Processing**: Reads JSON data files and converts to protobuf messages
4. **Kafka Publishing**: Sends GSR-encoded messages to Kafka topics

### Consumer

1. **GSR Deserialization**: Uses `deserializer.NewDeserializer()` for each message type
2. **Dynamic Message Handling**: Deserializes GSR-encoded data to dynamic protobuf messages
3. **Type Conversion**: Converts dynamic messages to concrete types using marshal/unmarshal pattern
4. **Multi-topic Consumption**: Consumes from multiple topics concurrently with graceful shutdown

### GSR Integration Pattern

The demo follows the same pattern as the integration tests:

```go
// Get message descriptor via reflection
messageDescriptor := message.ProtoReflect().Descriptor()

// Create GSR configuration
configMap := map[string]interface{}{
    common.DataFormatTypeKey:            common.DataFormatProtobuf,
    common.ProtobufMessageDescriptorKey: messageDescriptor,
}
config := common.NewConfiguration(configMap)

// Create serializer/deserializer
serializer, err := serializer.NewSerializer(config)
deserializer, err := deserializer.NewDeserializer(config)
```

## Data Format

The demo expects JSON data files in the `data/` directory:

- `data/users.json`: Array of user objects
- `data/orders.json`: Array of order objects

## AWS Setup

Ensure you have:

1. AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
2. AWS Glue Schema Registry set up in your region
3. Appropriate permissions to read/write schemas

## Troubleshooting

### Common Issues

1. **Import errors**: Run `go mod tidy` to resolve dependencies
2. **Protobuf generation**: Ensure `protoc` is installed and in PATH
3. **AWS permissions**: Check GSR permissions in AWS console
4. **Kafka connectivity**: Verify Kafka is running and accessible

### Debugging

- Enable verbose logging by modifying log levels in the code
- Check AWS CloudWatch logs for GSR-related issues
- Use Kafka tools to verify topic creation and message delivery

## Development

### Adding New Message Types

1. Add .proto file to `protos/` directory
2. Update `pkg/proto/generate.go` with new proto file
3. Run `go generate ./pkg/proto`
4. Add serialization/deserialization logic in producer/consumer
5. Create corresponding JSON data file

### Testing

The implementation follows the same patterns as the integration tests in `../golang/integration-tests/`. Reference those tests for validation and troubleshooting.
