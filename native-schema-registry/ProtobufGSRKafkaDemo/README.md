# ProtobufGSRKafkaDemo

Multi-threaded Protobuf GSR Kafka demonstration with 5 message types.

## Architecture

- **Producer**: 5 raw threads, each producing different message types to separate topics
- **Consumer**: 5 KafkaFlow consumers, each handling different message types
- **GSR Integration**: Uses high-level `GlueSchemaRegistryKafkaSerializer`/`GlueSchemaRegistryKafkaDeserializer`

## Message Types

1. **User** (simple) → `users` topic
2. **Product** (simple) → `products` topic  
3. **Order** (complex) → `orders` topic
4. **Payment** (complex) → `payments` topic
5. **Event** (complex) → `events` topic

## Setup

1. Start Kafka (uses included docker-compose.yml):
   ```bash
   ./start-kafka.sh
   ```

2. Build projects:
   ```bash
   dotnet build Shared/Shared.csproj
   dotnet build Producer/Producer.csproj
   dotnet build Consumer/Consumer.csproj
   ```

## Running

1. Start Kafka (if not already running):
   ```bash
   ./start-kafka.sh
   ```

2. Clean topics (for idempotent runs):
   ```bash
   ./cleanup-topics.sh
   ```

3. Start Producer (sends 10 messages per type = 50 total):
   ```bash
   cd Producer
   ./run-producer.sh
   ```

4. Start Consumer (in separate terminal):
   ```bash
   cd Consumer
   ./run-consumer.sh
   ```

## Configuration

- GSR settings in `config/gsr.properties`
- Requires AWS credentials and existing GSR registry
- Uses FORWARD compatibility for schema evolution

## Message Counts

- **Total Messages**: 50 (10 per message type)
- **Producer**: Each thread sends 10 messages then completes
- **Consumer**: Receives all messages and continues listening

## Threading

- **Producer**: 5 raw threads with separate serializer instances
- **Consumer**: KafkaFlow manages consumption with separate deserializer instances
- **Cleanup**: Proper disposal of all GSR resources on shutdown
- **Idempotent**: Use `./cleanup-topics.sh` between runs