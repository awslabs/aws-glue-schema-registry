package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

public class SchemaReplicationSchemaRegistryConstants {
    /**
     * AWS source endpoint to use while initializing the client for service.
     */
    public static final String AWS_SOURCE_ENDPOINT = "source.endpoint";
    /**
     * AWS source region to use while initializing the client for service.
     */
    public static final String AWS_SOURCE_REGION = "source.region";
    /**
     * AWS target endpoint to use while initializing the client for service.
     */
    public static final String AWS_TARGET_ENDPOINT = "target.endpoint";
    /**
     * AWS target region to use while initializing the client for service.
     */
    public static final String AWS_TARGET_REGION = "target.region";
    /**
     * Number of schema versions to replicate from source to target
     */
    public static final String REPLICATE_SCHEMA_VERSION_COUNT = "replicateSchemaVersionCount";
    /**
     * Default number of schema versions to replicate from source to target
     */
    public static final Integer DEFAULT_REPLICATE_SCHEMA_VERSION_COUNT = 10;
    /**
     * Source Registry Name.
     */
    public static final String SOURCE_REGISTRY_NAME = "source.registry.name";
    /**
     * Target Registry Name.
     */
    public static final String TARGET_REGISTRY_NAME = "target.registry.name";
}
