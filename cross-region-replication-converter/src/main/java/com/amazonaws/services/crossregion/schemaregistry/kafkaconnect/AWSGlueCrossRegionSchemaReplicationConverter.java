package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.Data;
import lombok.NonNull;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


@Data
public class AWSGlueCrossRegionSchemaReplicationConverter implements Converter {

    private AwsCredentialsProvider credentialsProvider;
    private GlueSchemaRegistryDeserializerImpl deserializer;
    private GlueSchemaRegistrySerializerImpl serializer;
    private boolean isKey;
    private Map<String, Object> sourceConfigs;
    private Map<String, Object> targetConfigs;

    /**
     * Constructor used by Kafka Connect user.
     */
    public AWSGlueCrossRegionSchemaReplicationConverter(){};

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

        if (configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_REGION) != null) {
            sourceConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_REGION));
        }
        if (configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT) != null) {
            sourceConfigs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT));
        }
        if (configs.get(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME) != null) {
            sourceConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, configs.get(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME));
        }

        if (configs.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION) != null) {
            targetConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, configs.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION));
        }
        if (configs.get(AWSSchemaRegistryConstants.TARGET_REGISTRY_NAME) != null) {
            targetConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, configs.get(AWSSchemaRegistryConstants.TARGET_REGISTRY_NAME));
        }
        if (configs.get(AWSSchemaRegistryConstants.AWS_TARGET_ENDPOINT) != null) {
            targetConfigs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, configs.get(AWSSchemaRegistryConstants.AWS_TARGET_ENDPOINT));
        }

        targetConfigs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);


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

    private void validateRequiredConfigsIfPresent(Map<String, ?> configs) {
        if (configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_REGION) == null) {
            throw new DataException("Source Region is not provided.");
        } else if (configs.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION) == null && configs.get(AWSSchemaRegistryConstants.AWS_REGION) == null) {
            throw new DataException("Target Region is not provided.");
        } else if (configs.get(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME) == null) {
            throw new DataException("Source Registry is not provided.");
        } else if (configs.get(AWSSchemaRegistryConstants.TARGET_REGISTRY_NAME) == null && configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME) == null) {
            throw new DataException("Target Registry is not provided.");
        } else if (configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT) == null) {
            throw new DataException("Source Endpoint is not provided.");
        } else if (configs.get(AWSSchemaRegistryConstants.AWS_TARGET_ENDPOINT) == null && configs.get(AWSSchemaRegistryConstants.AWS_ENDPOINT) == null) {
            throw new DataException("Target Endpoint is not provided.");
        }
    }

    public Map<Schema, UUID> createSchemaAndRegisterAllSchemaVersions(
            @NonNull Schema schema) throws AWSSchemaRegistryException {
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(sourceConfigs);
        AWSSchemaRegistryClient client = new AWSSchemaRegistryClient(credentialsProvider, glueSchemaRegistryConfiguration);

        GetSchemaResponse schemaResponse = client.getSchemaResponse(SchemaId.builder()
                .schemaName(schema.getSchemaName())
                .registryName(glueSchemaRegistryConfiguration.getSourceRegistryName())
                .build());

        Compatibility compatibility = schemaResponse.compatibility();
        glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(targetConfigs);
        client = new AWSSchemaRegistryClient(credentialsProvider, glueSchemaRegistryConfiguration);

        //TODO: Make a schema call to get the compatibility mode
        //TODO: Get all the metadata for the schema version

        //Just register the schema, no need to cache them. when MM2 reads the data, will retrieve the schema and will cache it.
        //When schema is not created in the target, create the schema in target and
        //register all the existing schema version from the source schema to the target in the same order.
        Map<Schema, UUID> schemaWithVersionId =
                client.createSchemaAndRegisterAllSchemaVersions(schema.getSchemaName(),
                        schema.getDataFormat(),
                        schema.getSchemaDefinition(),
                        compatibility,
                        new HashMap<>());

        return schemaWithVersionId;
    }
}
