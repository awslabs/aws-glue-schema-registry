#!/bin/bash
echo "Starting Kafka and Zookeeper..."

# Start Kafka services using local docker-compose.yml
docker-compose up -d

echo "Waiting for Kafka to start (30 seconds)..."
sleep 30

# Verify Kafka is running
echo "Checking Kafka status..."
docker ps | grep kafka

if docker ps | grep -q kafka; then
    echo "✅ Kafka is running on localhost:9092"
else
    echo "❌ Kafka failed to start"
    exit 1
fi