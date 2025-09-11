package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.MetadataInfo;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionListItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


@Data
@Slf4j
public class AWSGlueCrossRegionSchemaReplicationConverter implements Converter {

    private AwsCredentialsProvider credentialsProvider;
    private GlueSchemaRegistryDeserializerImpl deserializer;
    private GlueSchemaRegistrySerializerImpl serializer;
    private boolean isKey;
    private Map<String, Object> sourceConfigs;
    private Map<String, Object> targetConfigs;
    private SchemaReplicationGlueSchemaRegistryConfiguration targetGlueSchemaRegistryConfiguration;
    private SchemaReplicationGlueSchemaRegistryConfiguration sourceGlueSchemaRegistryConfiguration;

    @NonNull
    private AWSSchemaRegistryClient targetSchemaRegistryClient;
    @NonNull
    private AWSSchemaRegistryClient sourceSchemaRegistryClient;

    @NonNull
    @VisibleForTesting
    protected LoadingCache<Schema, UUID> schemaDefinitionToVersionCache;


    /**
     * Constructor used by Kafka Connect user.
     */
    public AWSGlueCrossRegionSchemaReplicationConverter(){}

    /**
     * Constructor accepting AWSCredentialsProvider.
     *
     * @param credentialsProvider AWSCredentialsProvider instance.
     */
    public AWSGlueCrossRegionSchemaReplicationConverter(
            AwsCredentialsProvider credentialsProvider,
            GlueSchemaRegistryDeserializerImpl deserializerImpl,
            GlueSchemaRegistrySerializerImpl serializerImpl) {

        this.credentialsProvider = credentialsProvider;
        this.deserializer = deserializerImpl;
        this.serializer = serializerImpl;
    }

    /**
     * Constructor accepting AWSSchemaRegistryClient.
     *
     * @param sourceSchemaRegistryClient AWSSchemaRegistryClient instance.
     * @param targetSchemaRegistryClient AWSSchemaRegistryClient instance.
     */
    public AWSGlueCrossRegionSchemaReplicationConverter(
            AWSSchemaRegistryClient sourceSchemaRegistryClient,
            AWSSchemaRegistryClient targetSchemaRegistryClient,
            GlueSchemaRegistryDeserializerImpl deserializerImpl,
            GlueSchemaRegistrySerializerImpl serializerImpl) {

        this.sourceSchemaRegistryClient = sourceSchemaRegistryClient;
        this.targetSchemaRegistryClient = targetSchemaRegistryClient;
        this.deserializer = deserializerImpl;
        this.serializer = serializerImpl;
    }

    /**
     * Configure the Schema Replication Converter.
     * @param configs configuration elements for the converter
     * @param isKey true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        if (credentialsProvider == null) {
            credentialsProvider = DefaultCredentialsProvider.builder().build();
        }

        // Put the source and target regions into configurations respectively
        sourceConfigs = new HashMap<>(configs);
        targetConfigs = new HashMap<>(configs);

        validateRequiredConfigsIfPresent(configs);

        if (configs.get(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION) != null) {
            sourceConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, configs.get(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION));
        }
        if (configs.get(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_ENDPOINT) != null) {
            sourceConfigs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, configs.get(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_ENDPOINT));
        }
        if (configs.get(SchemaReplicationSchemaRegistryConstants.SOURCE_REGISTRY_NAME) != null) {
            sourceConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, configs.get(SchemaReplicationSchemaRegistryConstants.SOURCE_REGISTRY_NAME));
        }

        if (configs.get(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_REGION) != null) {
            targetConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, configs.get(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_REGION));
        }
        if (configs.get(SchemaReplicationSchemaRegistryConstants.TARGET_REGISTRY_NAME) != null) {
            targetConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, configs.get(SchemaReplicationSchemaRegistryConstants.TARGET_REGISTRY_NAME));
        }
        if (configs.get(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_ENDPOINT) != null) {
            targetConfigs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, configs.get(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_ENDPOINT));
        }

        targetConfigs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        targetGlueSchemaRegistryConfiguration = new SchemaReplicationGlueSchemaRegistryConfiguration(targetConfigs);
        sourceGlueSchemaRegistryConfiguration = new SchemaReplicationGlueSchemaRegistryConfiguration(sourceConfigs);

        this.schemaDefinitionToVersionCache = CacheBuilder.newBuilder()
                .maximumSize(targetGlueSchemaRegistryConfiguration.getCacheSize())
                .refreshAfterWrite(targetGlueSchemaRegistryConfiguration.getTimeToLiveMillis(), TimeUnit.MILLISECONDS)
                .build(new SchemaDefinitionToVersionCache());

        if (targetSchemaRegistryClient == null) {
            targetSchemaRegistryClient = new AWSSchemaRegistryClient(credentialsProvider, targetGlueSchemaRegistryConfiguration);
        }
        if (sourceSchemaRegistryClient == null) {
            sourceSchemaRegistryClient = new AWSSchemaRegistryClient(credentialsProvider, sourceGlueSchemaRegistryConfiguration);
        }

        if (serializer == null) {
            serializer = new GlueSchemaRegistrySerializerImpl(credentialsProvider, targetGlueSchemaRegistryConfiguration);
        }
        if (deserializer == null) {
            deserializer = new GlueSchemaRegistryDeserializerImpl(credentialsProvider, sourceGlueSchemaRegistryConfiguration);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, org.apache.kafka.connect.data.Schema schema, Object value) {
        if (value == null) return null;
        byte[] bytes = (byte[]) value;

        try {
            byte[] deserializedBytes = deserializer.getData(bytes);
            Schema deserializedSchema = deserializer.getSchema(bytes);
            createSchemaAndRegisterAllSchemaVersions(deserializedSchema);
            return serializer.encode(topic, deserializedSchema, deserializedBytes);
        } catch (GlueSchemaRegistryIncompatibleDataException ex) {
            //This exception is raised when the header bytes don't have schema id, version byte or compression byte
            //This determines the data doesn't have schema information in it, so the actual message is returned.
            return bytes;
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization/deserialization error: ", e);
        } catch (ExecutionException e) {
            throw new DataException("Unknown Error: ", e);
        }
    }

    private void validateRequiredConfigsIfPresent(Map<String, ?> configs) {
        if (configs.get(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION) == null) {
            throw new DataException("Source Region is not provided.");
        } else if (configs.get(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_REGION) == null && configs.get(AWSSchemaRegistryConstants.AWS_REGION) == null) {
            throw new DataException("Target Region is not provided.");
        } else if (configs.get(SchemaReplicationSchemaRegistryConstants.SOURCE_REGISTRY_NAME) == null) {
            throw new DataException("Source Registry is not provided.");
        } else if (configs.get(SchemaReplicationSchemaRegistryConstants.TARGET_REGISTRY_NAME) == null && configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME) == null) {
            throw new DataException("Target Registry is not provided.");
        } else if (configs.get(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_ENDPOINT) == null) {
            throw new DataException("Source Endpoint is not provided.");
        } else if (configs.get(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_ENDPOINT) == null && configs.get(AWSSchemaRegistryConstants.AWS_ENDPOINT) == null) {
            throw new DataException("Target Endpoint is not provided.");
        }
    }

    /**
     * This method is not intended to be used for the CrossRegionReplicationConverter given it is integrated with a source connector
     *
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        throw new UnsupportedOperationException("This method is not supported");
    }

    @VisibleForTesting
    UUID createSchemaAndRegisterAllSchemaVersions(
            @NonNull Schema schema) throws AWSSchemaRegistryException, ExecutionException {

        UUID schemaVersionId;

        try {
            return schemaDefinitionToVersionCache.get(schema);
        } catch (Exception ex) {
            Map<Schema, UUID> schemaWithVersionId = new HashMap<>();
            String schemaName = schema.getSchemaName();
            String schemaNameFromArn = "";
            String schemaDefinition = "";
            String dataFormat = schema.getDataFormat();
            Map<String, String> metadataInfo = new HashMap<>();
            GetSchemaVersionResponse schemaVersionResponse = null;

            //Get compatibility mode and description for each schema
            SchemaId schemaId = SchemaId.builder()
                .schemaName(schema.getSchemaName())
                .registryName(sourceGlueSchemaRegistryConfiguration.getSourceRegistryName())
                .build();
            GetSchemaResponse schemaResponse = sourceSchemaRegistryClient.getSchemaResponse(schemaId);
            Compatibility compatibility = schemaResponse.compatibility();
            String description = schemaResponse.description();

            targetGlueSchemaRegistryConfiguration.setCompatibilitySetting(compatibility);
            targetGlueSchemaRegistryConfiguration.setDescription(description);
            targetSchemaRegistryClient.setGlueSchemaRegistryConfiguration(targetGlueSchemaRegistryConfiguration);

            try {

                //Get list of all schema versions
                List<SchemaVersionListItem> schemaVersionList = getSchemaVersionsOrderedByVersionNumber(schemaName, targetGlueSchemaRegistryConfiguration.getReplicateSchemaVersionCount());

                for (int idx = 0; idx < schemaVersionList.size(); idx++) {
                    //Get details of each schema versions
                    schemaVersionResponse =
                            sourceSchemaRegistryClient.getSchemaVersionResponse(schemaVersionList.get(idx).schemaVersionId());

                    schemaNameFromArn = getSchemaNameFromArn(schemaVersionList.get(idx).schemaArn());
                    schemaDefinition = schemaVersionResponse.schemaDefinition();

                    //Get the metadata information for each version
                    QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse = sourceSchemaRegistryClient.querySchemaVersionMetadata(UUID.fromString(schemaVersionResponse.schemaVersionId()));
                    metadataInfo = getMetadataInfo(querySchemaVersionMetadataResponse.metadataInfoMap());
                    //Create the schema with the first schema version
                    if (idx == 0) {
                        //Create the schema
                        schemaVersionId = createSchema(schemaNameFromArn, schemaDefinition, dataFormat, metadataInfo, schemaVersionResponse);
                    } else {
                        //Register subsequent schema versions
                        schemaVersionId = targetSchemaRegistryClient.registerSchemaVersion(schemaVersionResponse.schemaDefinition(),
                                schemaNameFromArn, dataFormat, metadataInfo);
                    }

                    cacheAllSchemaVersions(schemaVersionId, schemaWithVersionId, schemaNameFromArn, schemaVersionResponse);
                }
            } catch (AlreadyExistsException e) {
                log.warn("Schema is already created, this could be caused by multiple producers/MM2 racing to auto-create schema.");
                schemaVersionId = targetSchemaRegistryClient.registerSchemaVersion(schemaDefinition, schemaName, dataFormat, metadataInfo);
                cacheAllSchemaVersions(schemaVersionId, schemaWithVersionId, schemaNameFromArn, schemaVersionResponse);
                targetSchemaRegistryClient.putSchemaVersionMetadata(schemaVersionId, metadataInfo);
            } catch (Exception e) {
                String errorMessage = String.format("Exception occurred while fetching or registering schema name = %s ", schemaName);
                throw new AWSSchemaRegistryException(errorMessage, e);
            }
        }

        schemaVersionId = schemaDefinitionToVersionCache.get(schema);
        return schemaVersionId;
    }

    private void cacheAllSchemaVersions(UUID schemaVersionId, Map<Schema, UUID> schemaWithVersionId, String schemaNameFromArn, GetSchemaVersionResponse getSchemaVersionResponse) {
        Schema schemaVersionSchema = new Schema(getSchemaVersionResponse.schemaDefinition(), getSchemaVersionResponse.dataFormat().toString(), schemaNameFromArn);

        //Create a map of schema and schemaVersionId
        schemaWithVersionId.put(schemaVersionSchema, schemaVersionId);
        //Cache all the schema versions for a Glue Schema Registry schema
        schemaWithVersionId.entrySet()
                .stream()
                .forEach(item -> {
                    schemaDefinitionToVersionCache.put(item.getKey(), item.getValue());
                });
    }

    private UUID createSchema(String schemaNameFromArn, String schemaDefinition, String dataFormat, Map<String, String> metadataInfo, GetSchemaVersionResponse getSchemaVersionResponse) {
        UUID schemaVersionId;
        log.info("Auto Creating schema with schemaName: {} and schemaDefinition : {}",
                schemaNameFromArn, getSchemaVersionResponse.schemaDefinition());

        schemaVersionId = targetSchemaRegistryClient.createSchema(
                schemaNameFromArn,
                dataFormat,
                schemaDefinition, metadataInfo);

        return schemaVersionId;
    }

    public List<SchemaVersionListItem> getSchemaVersionsOrderedByVersionNumber(String schemaName, Integer replicateSchemaVersionCount) {
        //Copy the schemaVersionList to a new list as the existing list is not modifiable.
        List<SchemaVersionListItem> schemaVersionList = sourceSchemaRegistryClient.getSchemaVersions(schemaName);
        List<SchemaVersionListItem> modifiableSchemaVersionList = new ArrayList<>(schemaVersionList);

        //Sort the schemaVersionList based on versionNumber in ascending order.
        //This is important as the item in the list are in random order
        //and we need to maintain the ordering of versions
        Collections.sort(modifiableSchemaVersionList, Comparator.comparing(SchemaVersionListItem::versionNumber));

        //Get the list of schema versions equal to the replicateSchemaVersionCount
        //If the list is smaller than replicateSchemaVersionCount, return the whole list.
        modifiableSchemaVersionList = modifiableSchemaVersionList.subList(0,
                Math.min(replicateSchemaVersionCount, modifiableSchemaVersionList.size()));

        return modifiableSchemaVersionList;
    }

    private String getSchemaNameFromArn(String schemaArn) {
        String[] tokens = schemaArn.split(Pattern.quote("/"));
        return tokens[tokens.length - 1];
    }

    private Map<String, String> getMetadataInfo(Map<String, MetadataInfo> metadataInfoMap) {
        Map<String, String> metadata = new HashMap<>();
        Iterator<Map.Entry<String, MetadataInfo>> iterator = metadataInfoMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, MetadataInfo> entry = iterator.next();
            metadata.put(entry.getKey(), entry.getValue().metadataValue());
        }

        return metadata;
    }

    @RequiredArgsConstructor
    private class SchemaDefinitionToVersionCache extends CacheLoader<Schema, UUID> {
        @Override
        public UUID load(Schema schema) {
            return targetSchemaRegistryClient.getSchemaVersionIdByDefinition(
                    schema.getSchemaDefinition(), schema.getSchemaName(), schema.getDataFormat());
        }
    }
}
