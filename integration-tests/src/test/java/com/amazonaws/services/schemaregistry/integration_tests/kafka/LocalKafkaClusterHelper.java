package com.amazonaws.services.schemaregistry.integration_tests.kafka;

public class LocalKafkaClusterHelper implements KafkaClusterHelper {
    private static final String FAKE_CLUSTER_ARN = "FAKE_CLUSTER_ARN";
    private static final String BOOTSTRAP_STRING = "127.0.0.1:9092";
    private static final String ZOOKEEPER_STRING = "127.0.0.1:2181";
    private static final int NUMBER_OF_PARTITIONS = 1;
    private static final short REPLICATION_FACTOR = 1;

    @Override
    public String getOrCreateCluster() {
        return FAKE_CLUSTER_ARN;
    }

    @Override
    public String getBootstrapString() {
        return BOOTSTRAP_STRING;
    }

    @Override
    public String getZookeeperConnectString() {
        return ZOOKEEPER_STRING;
    }

    @Override
    public int getNumberOfPartitions() {
        return NUMBER_OF_PARTITIONS;
    }

    @Override
    public short getReplicationFactor() {
        return REPLICATION_FACTOR;
    }
}

