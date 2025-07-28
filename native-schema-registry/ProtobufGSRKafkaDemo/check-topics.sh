#!/bin/bash
echo "Checking Kafka topics and messages..."

echo "=== Available Topics ==="
sudo docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo -e "\n=== Topic Details ==="
for topic in users products orders payments events; do
    echo "--- $topic ---"
    sudo docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $topic --time -1
done

echo -e "\n=== Consumer Groups ==="
sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

echo -e "\n=== Consumer Group Details ==="
for group in user-consumer-group product-consumer-group order-consumer-group payment-consumer-group event-consumer-group; do
    echo "--- $group ---"
    sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $group 2>/dev/null || echo "Group not found"
done