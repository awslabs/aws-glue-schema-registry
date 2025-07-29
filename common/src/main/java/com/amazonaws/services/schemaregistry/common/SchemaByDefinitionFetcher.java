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
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Fetches the schema version for the given schema definition optionally registering the schema if required.
 */
@Slf4j
public class SchemaByDefinitionFetcher {
    @NonNull
    private final AWSSchemaRegistryClient awsSchemaRegistryClient;

    @NonNull
    private final GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;

    @NonNull
    @VisibleForTesting
    protected final LoadingCache<Schema, UUID> schemaDefinitionToVersionCache;

    public SchemaByDefinitionFetcher(
        final AWSSchemaRegistryClient awsSchemaRegistryClient,
        final GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {
        this.awsSchemaRegistryClient = awsSchemaRegistryClient;
        this.glueSchemaRegistryConfiguration = glueSchemaRegistryConfiguration;

        this.schemaDefinitionToVersionCache = CacheBuilder.newBuilder()
            .maximumSize(glueSchemaRegistryConfiguration.getCacheSize())
            .refreshAfterWrite(glueSchemaRegistryConfiguration.getTimeToLiveMillis(), TimeUnit.MILLISECONDS)
            .build(new SchemaDefinitionToVersionCache());
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

        // Log the attempt with schema details (truncated for readability)
        String truncatedSchema = schemaDefinition.length() > 100 ? 
            schemaDefinition.substring(0, 100) + "..." : schemaDefinition;
        log.info("GSR: Attempting schema lookup - name={}, dataFormat={}, cacheSize={}, schemaPreview={}", 
                 schemaName, dataFormat, schemaDefinitionToVersionCache.size(), truncatedSchema);

        try {
            UUID cachedVersion = schemaDefinitionToVersionCache.get(schema);
            log.info("GSR: CACHE HIT - Found schema version in cache: {} for schema: {}", cachedVersion, schemaName);
            return cachedVersion;
        } catch (Exception ex) {
            log.info("GSR: CACHE MISS - Schema not in cache, will attempt API lookup/registration for schema: {}", schemaName);
            Throwable schemaRegistryException = ex.getCause();
            String exceptionCauseMessage = schemaRegistryException.getCause().getMessage();

            if (exceptionCauseMessage.contains(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG)) {
                log.info("GSR: Schema version not found, attempting to register new version - schema: {}", schemaName);
                if (!glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled()) {
                    log.warn("GSR: Auto-registration is DISABLED, cannot register schema version - schema: {}", schemaName);
                    throw new AWSSchemaRegistryException(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG,
                        schemaRegistryException);
                }
                log.info("GSR: Making API call to registerSchemaVersion - schema: {}", schemaName);
                schemaVersionId =
                    awsSchemaRegistryClient.registerSchemaVersion(schemaDefinition, schemaName, dataFormat, metadata);
                log.info("GSR: Successfully registered schema version: {} for schema: {}", schemaVersionId, schemaName);
            } else if (exceptionCauseMessage.contains(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)) {
                log.info("GSR: Schema not found, attempting to create new schema - schema: {}", schemaName);
                if (!glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled()) {
                    log.warn("GSR: Auto-registration is DISABLED, cannot create schema - schema: {}", schemaName);
                    throw new AWSSchemaRegistryException(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG,
                        schemaRegistryException);
                }
                log.info("GSR: Making API call to createSchema - schema: {}", schemaName);
                schemaVersionId =
                    awsSchemaRegistryClient.createSchema(schemaName, dataFormat, schemaDefinition, metadata);
                log.info("GSR: Successfully created schema with version: {} for schema: {}", schemaVersionId, schemaName);
            } else {
                String msg =
                    String.format(
                        "Exception occurred while fetching or registering schema definition = %s, schema name = %s ",
                        schemaDefinition, schemaName);
                throw new AWSSchemaRegistryException(msg, schemaRegistryException);
            }
            log.info("GSR: Caching schema version: {} for schema: {}", schemaVersionId, schemaName);
            schemaDefinitionToVersionCache.put(schema, schemaVersionId);
        }
        return schemaVersionId;
    }

    @RequiredArgsConstructor
    private class SchemaDefinitionToVersionCache extends CacheLoader<Schema, UUID> {
        @Override
        public UUID load(Schema schema) {
            log.info("GSR: Cache loader - Making API call to getSchemaVersionIdByDefinition for schema: {}", schema.getSchemaName());
            UUID result = awsSchemaRegistryClient.getSchemaVersionIdByDefinition(
                schema.getSchemaDefinition(), schema.getSchemaName(), schema.getDataFormat());
            log.info("GSR: Cache loader - API call completed, received schema version: {} for schema: {}", result, schema.getSchemaName());
            return result;
        }
    }
}
