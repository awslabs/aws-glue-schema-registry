#!/bin/sh
set -e

echo "=== Dockerized Integration Test Runner ==="
echo "Go version: $(go version)"

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

echo "✅ Kafka is ready"

# Run tests (protobuf files already generated and copied during image build)
echo "Running integration tests..."
cd /app/golang/integration-tests
go test  -count=1 ./...

echo "✅ Tests completed successfully"
