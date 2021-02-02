package com.amazonaws.services.schemaregistry.integration_tests.kafka;

public interface KafkaClusterHelper {
    String getOrCreateCluster();

    String getBootstrapString();

    String getZookeeperConnectString();

    int getNumberOfPartitions();

    short getReplicationFactor();
}