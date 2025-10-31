#!/bin/bash

BS="kafka-1:9092,kafka-2:9092,kafka-3:9092"

for t in users products orders payments events; do
  /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BS" --create --if-not-exists \
    --topic "$t" --partitions 6 --replication-factor 2
done
