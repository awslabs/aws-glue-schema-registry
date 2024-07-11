package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import software.amazon.awssdk.services.glue.model.Compatibility;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Fetches the schema version for the given schema definition optionally registering the schema if required.
 */
public class SchemaByDefinitionFetcher {
    @NonNull
    private final AWSSchemaRegistryClient awsSchemaRegistryClient;

    @NonNull
    private final GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;

    @NonNull
    @VisibleForTesting
    protected final LoadingCache<Schema, UUID> schemaDefinitionToVersionCache;

    @NonNull
    @VisibleForTesting
    protected final LoadingCache<SchemaV2, UUID> schemaDefinitionToVersionCacheV2;

    public SchemaByDefinitionFetcher(
        final AWSSchemaRegistryClient awsSchemaRegistryClient,
        final GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {
        this.awsSchemaRegistryClient = awsSchemaRegistryClient;
        this.glueSchemaRegistryConfiguration = glueSchemaRegistryConfiguration;

        this.schemaDefinitionToVersionCache = CacheBuilder.newBuilder()
            .maximumSize(glueSchemaRegistryConfiguration.getCacheSize())
            .refreshAfterWrite(glueSchemaRegistryConfiguration.getTimeToLiveMillis(), TimeUnit.MILLISECONDS)
            .build(new SchemaDefinitionToVersionCache());

        this.schemaDefinitionToVersionCacheV2 = CacheBuilder.newBuilder()
                .maximumSize(glueSchemaRegistryConfiguration.getCacheSize())
                .refreshAfterWrite(glueSchemaRegistryConfiguration.getTimeToLiveMillis(), TimeUnit.MILLISECONDS)
                .build(new SchemaDefinitionToVersionCacheV2());
    }

    /**
     * Get Schema Version ID by following below steps :
     * <p>
     * 1) If schema version id exists in registry then get it from registry
     * 2) If schema version id does not exist in registry
     * then if auto registration is enabled
     * then if schema exists but version doesn't exist
     * then
     * 2.1) Register schema version
     * else if schema does not exist
     * then
     * 2.2) create schema and register schema version
     *
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @param dataFormat       Data Format
     * @param metadata         metadata for schema version
     * @return Schema Version ID
     * @throws AWSSchemaRegistryException on any error while fetching the schema version ID
     */
    @SneakyThrows
    public UUID getORRegisterSchemaVersionId(
        @NonNull String schemaDefinition,
        @NonNull String schemaName,
        @NonNull String dataFormat,
        @NonNull Map<String, String> metadata) throws AWSSchemaRegistryException {
        UUID schemaVersionId;
        final Schema schema = new Schema(schemaDefinition, dataFormat, schemaName);

        try {
            return schemaDefinitionToVersionCache.get(schema);
        } catch (Exception ex) {
            Throwable schemaRegistryException = ex.getCause();
            String exceptionCauseMessage = schemaRegistryException.getCause().getMessage();

            if (exceptionCauseMessage.contains(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG)) {
                if (!glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled()) {
                    throw new AWSSchemaRegistryException(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG,
                        schemaRegistryException);
                }
                schemaVersionId =
                    awsSchemaRegistryClient.registerSchemaVersion(schemaDefinition, schemaName, dataFormat, metadata);
            } else if (exceptionCauseMessage.contains(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)) {
                if (!glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled()) {
                    throw new AWSSchemaRegistryException(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG,
                        schemaRegistryException);
                }

                schemaVersionId =
                    awsSchemaRegistryClient.createSchema(schemaName, dataFormat, schemaDefinition, metadata);
            } else {
                String msg =
                    String.format(
                        "Exception occurred while fetching or registering schema definition = %s, schema name = %s ",
                        schemaDefinition, schemaName);
                throw new AWSSchemaRegistryException(msg, schemaRegistryException);
            }
            schemaDefinitionToVersionCache.put(schema, schemaVersionId);
        }
        return schemaVersionId;
    }

    /**
     * Get Schema Version ID by following below steps :
     * <p>
     * 1) If schema version id exists in registry then get it from registry
     * 2) If schema version id does not exist in registry
     * then if auto registration is enabled
     * then if schema exists but version doesn't exist
     * then
     * 2.1) Register schema version
     * else if schema does not exist
     * then
     * 2.2) create schema and register schema version
     *
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @param dataFormat       Data Format
     * @param metadata         metadata for schema version
     * @return Schema Version ID
     * @throws AWSSchemaRegistryException on any error while fetching the schema version ID
     */
    @SneakyThrows
    public UUID getORRegisterSchemaVersionIdV2(
            @NonNull String schemaDefinition,
            @NonNull String schemaName,
            @NonNull String dataFormat,
            @NonNull Compatibility compatibility,
            @NonNull Map<String, String> metadata) throws AWSSchemaRegistryException {
        UUID schemaVersionId;
        Map<SchemaV2, UUID> schemaWithVersionId;
        final SchemaV2 schema = new SchemaV2(schemaDefinition, dataFormat, schemaName, compatibility);

        try {
            return schemaDefinitionToVersionCacheV2.get(schema);
        } catch (Exception ex) {
            Throwable schemaRegistryException = ex.getCause();
            String exceptionCauseMessage = schemaRegistryException.getCause().getMessage();

            if (exceptionCauseMessage.contains(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG)) {
                if (!glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled()) {
                    throw new AWSSchemaRegistryException(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG,
                            schemaRegistryException);
                }
                schemaVersionId =
                        awsSchemaRegistryClient.registerSchemaVersion(schemaDefinition, schemaName, dataFormat, metadata);
                schemaDefinitionToVersionCacheV2.put(schema, schemaVersionId);
            } else if (exceptionCauseMessage.contains(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)) {
                if (!glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled()) {
                    throw new AWSSchemaRegistryException(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG,
                            schemaRegistryException);
                }

                //When schema is not created in the target, create the schema in target and
                // register all the existing schema version from the source schema to the target in the same order.
                schemaWithVersionId =
                        awsSchemaRegistryClient.createSchemaV2(schemaName, dataFormat, schemaDefinition, compatibility, metadata);

                //Cache all the schema versions for a Glue Schema Registry schema
                schemaWithVersionId.entrySet()
                                .stream()
                                .forEach(item -> {
                                    schemaDefinitionToVersionCacheV2.put(item.getKey(), item.getValue());
                                });
            } else {
                String msg =
                        String.format(
                                "Exception occurred while fetching or registering schema definition = %s, schema name = %s ",
                                schemaDefinition, schemaName);
                throw new AWSSchemaRegistryException(msg, schemaRegistryException);
            }
        }
        schemaVersionId = schemaDefinitionToVersionCacheV2.get(schema);
        return schemaVersionId;
    }

    @RequiredArgsConstructor
    private class SchemaDefinitionToVersionCache extends CacheLoader<Schema, UUID> {
        @Override
        public UUID load(Schema schema) {
            return awsSchemaRegistryClient.getSchemaVersionIdByDefinition(
                schema.getSchemaDefinition(), schema.getSchemaName(), schema.getDataFormat());
        }
    }

    @RequiredArgsConstructor
    private class SchemaDefinitionToVersionCacheV2 extends CacheLoader<SchemaV2, UUID> {
        @Override
        public UUID load(SchemaV2 schema) {
            return awsSchemaRegistryClient.getSchemaVersionIdByDefinition(
                    schema.getSchemaDefinition(), schema.getSchemaName(), schema.getDataFormat());
        }
    }
}