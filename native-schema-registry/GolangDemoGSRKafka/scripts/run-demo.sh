#!/bin/sh
set -e

echo "=== Dockerized Integration Test Runner ==="
echo "Go version: $(go version)"

# Run tests (protobuf files already generated and copied during image build)
cd /app/GolangDemoGSRKafka


go run ./cmd/producer/main.go -brokers $KAFKA_BROKER 2>&1  | tee ./logs/producer.log

go run ./cmd/consumer/main.go -brokers $KAFKA_BROKER 2>&1  | tee ./logs/consumer.log

exit
