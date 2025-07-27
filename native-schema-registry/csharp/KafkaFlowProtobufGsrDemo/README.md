# KafkaFlow Protobuf GSR Demo

A simple demonstration of using KafkaFlow with AWS Glue Schema Registry (GSR) for Protocol Buffers serialization and deserialization.

## Overview

This project demonstrates how to integrate KafkaFlow with AWS Glue Schema Registry for handling Protocol Buffers messages in Kafka. It includes both a producer and consumer application that work with 5 different data formats:

1. **Customer** - Customer management data
2. **Transaction** - Financial transaction records
3. **Inventory** - Product inventory tracking
4. **Notification** - User notification messages
5. **Analytics** - User behavior and analytics events

## Project Structure

```
KafkaFlowProtobufGsrDemo/
├── src/
│   ├── Messages/              # Shared protobuf message definitions
│   │   ├── Protos/           # .proto files
│   │   │   ├── customer.proto
│   │   │   ├── transaction.proto
│   │   │   ├── inventory.proto
│   │   │   ├── notification.proto
│   │   │   └── analytics.proto
│   │   └── Messages.csproj   # Generated C# classes from protos
│   ├── Producer/             # Message producer application
│   │   ├── Program.cs
│   │   ├── GsrProtobufSerializer.cs
│   │   ├── MessageProducerService.cs
│   │   └── Producer.csproj
│   └── Consumer/             # Message consumer application
│       ├── Program.cs
│       ├── GsrProtobufDeserializer.cs
│       ├── Consumer.csproj
│       └── MessageHandlers/  # Individual message handlers
│           ├── CustomerHandler.cs
│           ├── TransactionHandler.cs
│           ├── InventoryHandler.cs
│           ├── NotificationHandler.cs
│           └── AnalyticsHandler.cs
├── docker-compose.yml        # Local Kafka setup
└── README.md                 # This file
```

## Message Types

### 1. Customer
- **Topic**: `customers`
- **Fields**: ID, Name, Email, Status, Address, Created/Updated timestamps
- **Use Case**: Customer management and CRM data

### 2. Transaction
- **Topic**: `transactions`  
- **Fields**: ID, Customer ID, Amount, Currency, Type, Status, Payment Method, timestamps
- **Use Case**: Financial transactions and payment processing

### 3. Inventory
- **Topic**: `inventory`
- **Fields**: SKU, Name, Quantity, Price, Currency, Status, Location, Supplier, timestamps
- **Use Case**: Product inventory management

### 4. Notification
- **Topic**: `notifications`
- **Fields**: ID, Title, Message, Recipient, Type, Priority, Channel, Status, timestamps
- **Use Case**: User notifications and alerts

### 5. Analytics
- **Topic**: `analytics`
- **Fields**: ID, User ID, Event Name, Value, Properties (map), Session ID, timestamp
- **Use Case**: User behavior tracking and analytics

## Prerequisites

1. **.NET 8.0 SDK** or later
2. **Docker and Docker Compose** (for local Kafka)
3. **AWS Glue Schema Registry** access (or LocalStack for testing)
4. **AWS credentials** configured

## Setup

### 1. Build the AWSGsrSerDe Library

First, build the AWS GSR serialization/deserialization library:

```bash
cd ../AWSGsrSerDe
dotnet build
```

### 2. Start Local Kafka

```bash
docker-compose up -d
```

This will start:
- Zookeeper on port 2181
- Kafka on port 9092
- Kafdrop (Kafka UI) on port 9000

### 3. Build the Demo Projects

```bash
# Build all projects
dotnet build

# Or build individually
cd src/Messages && dotnet build
cd src/Producer && dotnet build  
cd src/Consumer && dotnet build
```

## Running the Demo

### 1. Start the Consumer

```bash
cd src/Consumer
dotnet run
```

The consumer will start listening to all 5 topics and log received messages.

### 2. Start the Producer

```bash
cd src/Producer
dotnet run
```

The producer will continuously send sample messages for all 5 data types to their respective topics.

## Configuration

### AWS Glue Schema Registry Configuration

The GSR configuration is set in both the serializer and deserializer:

```csharp
var config = new GlueSchemaRegistryConfiguration
{
    Region = "us-west-2",
    RegistryName = "KafkaFlow-Demo",
    DataFormat = DataFormat.PROTOBUF
};
```

### Kafka Configuration

Both applications connect to Kafka on `localhost:9092` by default. Update the broker configuration in `Program.cs` files if needed.

## Key Features

### Custom GSR Serializer/Deserializer

- **GsrProtobufSerializer**: Integrates AWS GSR serialization with KafkaFlow
- **GsrProtobufDeserializer**: Integrates AWS GSR deserialization with KafkaFlow
- Handles schema registration and evolution automatically
- Type-safe protobuf message handling

### Message Handlers

Each message type has its own dedicated handler that processes received messages:

- Structured logging of all message fields
- Type-safe message processing
- Easy to extend for business logic

### Schema Evolution

The protobuf schemas support evolution:
- Adding optional fields
- Deprecating fields  
- Maintaining backward compatibility

## Testing

### View Messages in Kafdrop

1. Open http://localhost:9000 in your browser
2. Select a topic to view messages
3. Messages will be displayed in binary format (protobuf encoded)

### Monitor Logs

Both applications use structured logging. Watch the console output to see:
- Producer: Messages being sent
- Consumer: Messages being received and processed

## Troubleshooting

### Common Issues

1. **AWS Credentials**: Ensure AWS credentials are properly configured
2. **GSR Permissions**: Verify access to the specified Glue Schema Registry
3. **Kafka Connection**: Ensure Kafka is running and accessible
4. **Build Errors**: Make sure AWSGsrSerDe is built first

### Schema Registry Issues

- Schemas are automatically registered on first use
- Check AWS Glue Console for registered schemas
- Ensure consistent schema evolution practices

## Extension Points

This demo can be extended to:

1. **Add More Message Types**: Create new .proto files and handlers
2. **Business Logic**: Add processing logic in message handlers  
3. **Error Handling**: Implement retry policies and dead letter queues
4. **Monitoring**: Add metrics and health checks
5. **Configuration**: Externalize configuration to appsettings.json
6. **Testing**: Add unit and integration tests

## Clean Up

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (optional)
docker-compose down -v
