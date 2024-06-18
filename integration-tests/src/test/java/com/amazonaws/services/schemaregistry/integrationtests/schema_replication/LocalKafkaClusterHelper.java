/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.schemaregistry.integrationtests.schema_replication;

public class LocalKafkaClusterHelper implements KafkaClusterHelper {
    private static final String FAKE_CLUSTER_ARN = "FAKE_CLUSTER_ARN";
    private static final String SRC_BOOTSTRAP_STRING = "127.0.0.1:9092";
    private static final String DEST_BOOTSTRAP_STRING = "127.0.0.1:9093";
    private static final int NUMBER_OF_PARTITIONS = 1;
    private static final short REPLICATION_FACTOR = 1;

    @Override
    public String getOrCreateCluster() {
        return FAKE_CLUSTER_ARN;
    }

    @Override
    public String getSrcClusterBootstrapString() {
        return SRC_BOOTSTRAP_STRING;
    }

    @Override
    public String getDestClusterBootstrapString() {
        return DEST_BOOTSTRAP_STRING;
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

