package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
public class AWSSChemaMultiRegionRegistryClient {
    private AWSSchemaRegistryClient sourceClient;
    private AWSSchemaRegistryClient targetClient;
}
