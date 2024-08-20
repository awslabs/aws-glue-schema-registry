package com.amazonaws.services.schemaregistry.common;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
public class AWSSChemaMultiRegionRegistryClient {
    private AWSSchemaRegistryClient sourceClient;
    private AWSSchemaRegistryClient targetClient;
}
