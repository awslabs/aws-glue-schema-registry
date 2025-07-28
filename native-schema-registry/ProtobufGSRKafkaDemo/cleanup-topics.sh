#!/bin/bash
echo "Cleaning up Kafka topics for idempotent runs..."

# Delete topics if they exist
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic users 2>/dev/null || true
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic products 2>/dev/null || true
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic orders 2>/dev/null || true
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic payments 2>/dev/null || true
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic events 2>/dev/null || true

echo "Topics cleaned up. Ready for fresh run."