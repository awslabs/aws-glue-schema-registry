package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
@Data
public class SchemaReplicationGlueSchemaRegistryConfiguration  extends GlueSchemaRegistryConfiguration {
    // TODO: Remove configs that are not useful non replication use-cases
    // https://github.com/awslabs/aws-glue-schema-registry/issues/292
    private String sourceEndPoint;
    private String sourceRegion;
    private String targetEndPoint;
    private String targetRegion;
    private String sourceRegistryName;
    private String targetRegistryName;
    private int replicateSchemaVersionCount;

    public SchemaReplicationGlueSchemaRegistryConfiguration(Map<String, ?> configs) {
        super(configs);
        buildSchemaReplicationSchemaRegistryConfigs(configs);
    }

    private void buildSchemaReplicationSchemaRegistryConfigs(Map<String, ?> configs) {
        validateAndSetAWSSourceRegion(configs);
        validateAndSetAWSTargetRegion(configs);
        validateAndSetAWSSourceEndpoint(configs);
        validateAndSetAWSTargetEndpoint(configs);
        validateAndSetSourceRegistryName(configs);
        validateAndSetTargetRegistryName(configs);
        validateAndSetReplicateSchemaVersionCount(configs);
    }

    private void validateAndSetAWSSourceRegion(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_SOURCE_REGION)) {
            this.sourceRegion = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_REGION));
        }
    }

    private void validateAndSetAWSTargetRegion(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_TARGET_REGION)) {
            this.targetRegion = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION));
        } else {
            this.targetRegion = this.getRegion();
        }
    }

    private void validateAndSetSourceRegistryName(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME)) {
            this.sourceRegistryName = String.valueOf(configs.get(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME));
        }
    }

    private void validateAndSetTargetRegistryName(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.TARGET_REGISTRY_NAME)) {
            this.targetRegistryName = String.valueOf(configs.get(AWSSchemaRegistryConstants.TARGET_REGISTRY_NAME));
        } else {
            this.targetRegistryName = this.getRegistryName();
        }
    }

    private void validateAndSetAWSSourceEndpoint(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT)) {
            this.sourceEndPoint = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT));
        }
    }

    private void validateAndSetAWSTargetEndpoint(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_TARGET_ENDPOINT)) {
            this.targetEndPoint = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_TARGET_ENDPOINT));
        } else {
            this.targetEndPoint = this.getEndPoint();
        }
    }

    private void validateAndSetReplicateSchemaVersionCount(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.REPLICATE_SCHEMA_VERSION_COUNT)) {
            this.replicateSchemaVersionCount = Integer.valueOf(configs.get(AWSSchemaRegistryConstants.REPLICATE_SCHEMA_VERSION_COUNT).toString());
        } else {
            this.replicateSchemaVersionCount = AWSSchemaRegistryConstants.DEFAULT_REPLICATE_SCHEMA_VERSION_COUNT;
        }
    }
}
