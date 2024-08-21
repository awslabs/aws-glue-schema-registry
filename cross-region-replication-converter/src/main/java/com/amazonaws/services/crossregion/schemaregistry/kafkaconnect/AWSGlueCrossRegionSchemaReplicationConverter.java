package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
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
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.MetadataInfo;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionListItem;

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

    @NonNull
    private AWSSchemaRegistryClient awsSchemaRegistryClient;

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
     * Configure the Schema Replication Converter.
     * @param configs configuration elements for the converter
     * @param isKey true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        // TODO: Support credentialProvider passed on by the user
        // https://github.com/awslabs/aws-glue-schema-registry/issues/293
        credentialsProvider = DefaultCredentialsProvider.builder().build();

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

        SchemaReplicationGlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new SchemaReplicationGlueSchemaRegistryConfiguration(targetConfigs);
        awsSchemaRegistryClient = new AWSSchemaRegistryClient(credentialsProvider, glueSchemaRegistryConfiguration);

        this.schemaDefinitionToVersionCache = CacheBuilder.newBuilder()
                .maximumSize(glueSchemaRegistryConfiguration.getCacheSize())
                .refreshAfterWrite(glueSchemaRegistryConfiguration.getTimeToLiveMillis(), TimeUnit.MILLISECONDS)
                .build(new SchemaDefinitionToVersionCache());

        serializer = new GlueSchemaRegistrySerializerImpl(credentialsProvider, new GlueSchemaRegistryConfiguration(targetConfigs));
        deserializer = new GlueSchemaRegistryDeserializerImpl(credentialsProvider, new GlueSchemaRegistryConfiguration(sourceConfigs));
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
        }  catch(GlueSchemaRegistryIncompatibleDataException ex) {
            //This exception is raised when the header bytes don't have schema id, version byte or compression byte
            //This determines the data doesn't have schema information in it, so the actual message is returned.
            return bytes;
        }
        catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization/deserialization error: ", e);
        } catch (ExecutionException e) {
            //TODO: Proper messaging and error handling
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization/deserialization error: ", e);
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

    public UUID createSchemaAndRegisterAllSchemaVersions(
            @NonNull Schema schema) throws AWSSchemaRegistryException, ExecutionException {
        SchemaReplicationGlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new SchemaReplicationGlueSchemaRegistryConfiguration(sourceConfigs);
        AWSSchemaRegistryClient sourceClient = new AWSSchemaRegistryClient(credentialsProvider, glueSchemaRegistryConfiguration);

        GetSchemaResponse schemaResponse = sourceClient.getSchemaResponse(SchemaId.builder()
                .schemaName(schema.getSchemaName())
                .registryName(glueSchemaRegistryConfiguration.getSourceRegistryName())
                .build());

        Compatibility compatibility = schemaResponse.compatibility();

        glueSchemaRegistryConfiguration = new SchemaReplicationGlueSchemaRegistryConfiguration(targetConfigs);
        glueSchemaRegistryConfiguration.setCompatibilitySetting(compatibility);
        AWSSchemaRegistryClient targetClient = new AWSSchemaRegistryClient(credentialsProvider, glueSchemaRegistryConfiguration);

        //TODO: Make a schema call to get the compatibility mode
        //TODO: Get all the metadata for the schema version

        //Just register the schema, no need to cache them. when MM2 reads the data, will retrieve the schema and will cache it.
        //When schema is not created in the target, create the schema in target and
        //register all the existing schema version from the source schema to the target in the same order.
//        Map<Schema, UUID> schemaWithVersionId =
//                multiRegionRegistryClient.getTargetClient().createSchemaAndRegisterAllSchemaVersions(schema.getSchemaName(),
//                        schema.getDataFormat(),
//                        schema.getSchemaDefinition(),
//                        compatibility,
//                        new HashMap<>());
//
//        return schemaWithVersionId;
        Map<Schema, UUID> schemaWithVersionId = new HashMap<>();
        String schemaName = schema.getSchemaName();
        String schemaNameFromArn = "";
        String schemaDefinition = "";
        String dataFormat = schema.getDataFormat();
        Map<String, String> metadataInfo = new HashMap<>();
        UUID schemaVersionId;

        try {
            return schemaDefinitionToVersionCache.get(schema);
        } catch(Exception ex) {
            try{
                //Get list of all schema versions
                List<SchemaVersionListItem> schemaVersionList = sourceClient.getSchemaVersions(schemaName, glueSchemaRegistryConfiguration.getReplicateSchemaVersionCount());

                for (int idx = 0; idx < schemaVersionList.size(); idx++){
                    //Get details of each schema versions
                    GetSchemaVersionResponse getSchemaVersionResponse =
                            sourceClient.getSchemaVersionResponse(schemaVersionList.get(idx).schemaVersionId());

                    schemaNameFromArn = getSchemaNameFromArn(schemaVersionList.get(idx).schemaArn());
                    schemaDefinition = getSchemaVersionResponse.schemaDefinition();
                    dataFormat = getSchemaVersionResponse.dataFormat().toString();

                    //Get the metadata information for each version
                    QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse = sourceClient.querySchemaVersionMetadata(UUID.fromString(getSchemaVersionResponse.schemaVersionId()));
                    metadataInfo = getMetadataInfo(querySchemaVersionMetadataResponse.metadataInfoMap());

                    //Create the schema with the first schema version
                    if (idx == 0) {
                        log.info("Auto Creating schema with schemaName: {} and schemaDefinition : {}",
                                schemaNameFromArn, getSchemaVersionResponse.schemaDefinition());

                        //Create the schema
                        schemaVersionId = targetClient.createSchema(
                                schemaNameFromArn,
                                dataFormat,
                                schemaDefinition, new HashMap<>()); //TODO: Get metadata of Schema

                        //Add version metadata to the schema version
                        targetClient.putSchemaVersionMetadata(schemaVersionId, metadataInfo);
                    } else {
                        //Register subsequent schema versions
                        schemaVersionId = targetClient.registerSchemaVersion(getSchemaVersionResponse.schemaDefinition(),
                                schemaNameFromArn, dataFormat, metadataInfo);
                    }

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
            }
            //TODO: will there be a scenario where duplicate schema creation logic can hit when MM2 trying to sync with all schemas
//            catch (AlreadyExistsException e) {
//                log.warn("Schema is already created, this could be caused by multiple producers racing to "
//                        + "auto-create schema.");
//                schemaVersionId = targetClient.registerSchemaVersion(schemaDefinition, schemaName, dataFormat, metadataInfo);
//                Schema schemaVersionSchema = new Schema(schemaDefinition, dataFormat, schemaName);
//                schemaWithVersionId.put(schemaVersionSchema, schemaVersionId);
//                targetClient.putSchemaVersionMetadata(schemaVersionId, metadataInfo);
//
//            }
            catch (Exception e) {
                String errorMessage = String.format(
                        "Create schema :: Call failed when creating the schema with the schema registry for"
                                + " schema name = %s", schemaName);
                //TODO: Will this exception be ever thrown?
                throw new AWSSchemaRegistryException(errorMessage, e);
            }
        }

        schemaVersionId = schemaDefinitionToVersionCache.get(schema);
        return schemaVersionId;
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
            return awsSchemaRegistryClient.getSchemaVersionIdByDefinition(
                    schema.getSchemaDefinition(), schema.getSchemaName(), schema.getDataFormat());
        }
    }
}
