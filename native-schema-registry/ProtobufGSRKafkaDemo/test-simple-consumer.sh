#!/bin/bash
echo "Testing simple Kafka consumer without GSR..."

# Test if basic Kafka consumption works
sudo docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic users --from-beginning --max-messages 5