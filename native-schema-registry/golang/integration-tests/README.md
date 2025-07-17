# Kafka Serializer Integration Tests

Integration tests for KafkaSerializer and KafkaDeserializer that test end-to-end serialization with AWS Glue Schema Registry and Kafka.

## Test Flow

```
1. KafkaSerializer.Serialize(protobuf) → GSR-encoded bytes (auto-registers schema)
2. Publish GSR-encoded bytes to Kafka topic
3. Consume GSR-encoded bytes from Kafka topic  
4. KafkaDeserializer.Deserialize(GSR-encoded-bytes) → original protobuf message
5. Validate round-trip success
```

## Prerequisites

### 1. AWS Credentials
Configure AWS credentials using environment variables:
```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

Or use AWS CLI:
```bash
aws configure
```

### 2. Kafka
Start Kafka:
```bash
cd native-schema-registry/golang/integration-tests
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
docker-compose up -d
```

### 3. Native Libraries
Build the GSR native libraries:
```bash
cd native-schema-registry/golang
make build
```

## Running Tests

```bash
cd native-schema-registry/golang/integration-tests

# Run all integration tests
go test -v ./...

# Skip integration tests (for CI)
SKIP_INTEGRATION_TESTS=true go test -v ./...

# Custom configuration
KAFKA_BROKER=localhost:9094 AWS_REGION=us-west-2 go test -v ./...
```

## Test Messages

Tests use protobuf types from `google.golang.org/protobuf/types/descriptorpb`:
- `FileDescriptorProto` - File descriptor messages
- `DescriptorProto` - Message descriptors  
- `FieldDescriptorProto` - Field descriptors

No custom protoc generation required.

## Components Tested

- **KafkaSerializer** - High-level serializer with AWS GSR integration
- **KafkaDeserializer** - High-level deserializer with AWS GSR integration

Both components handle AWS authentication and GSR operations internally. If AWS credentials are missing or invalid, KafkaSerializer.Serialize() will fail with appropriate error messages.

## Environment Variables

- `AWS_REGION` - AWS region for Glue Schema Registry (default: us-east-1)
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `KAFKA_BROKER` - Kafka broker address (default: localhost:9092)
- `SKIP_INTEGRATION_TESTS` - Set to "true" to skip tests
