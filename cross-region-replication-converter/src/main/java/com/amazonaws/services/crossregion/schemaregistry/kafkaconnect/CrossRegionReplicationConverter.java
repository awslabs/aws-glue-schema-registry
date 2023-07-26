package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

import lombok.Data;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

@Data
public class CrossRegionReplicationConverter implements Converter {

    private AwsCredentialsProvider credentialsProvider;
    private GlueSchemaRegistryDeserializerImpl deserializer;
    private GlueSchemaRegistrySerializerImpl serializer;
    private boolean isKey;

    /**
     * Constructor used by Kafka Connect user.
     */
    public CrossRegionReplicationConverter(){};

    /**
     * Constructor accepting AWSCredentialsProvider.
     *
     * @param credentialsProvider AWSCredentialsProvider instance.
     */
    public CrossRegionReplicationConverter(
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
        //TODO: Support credentialProvider passed on by the user
        credentialsProvider = DefaultCredentialsProvider.builder().build();

        // Put the source and target regions into configurations respectively
        Map<String, Object> sourceConfigs = new HashMap<>(configs);
        Map<String, Object> targetConfigs = new HashMap<>(configs);

        if (configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_REGION) == null){
            throw new DataException("Source Region is not provided.");
        }
        else if (configs.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION) == null && configs.get(AWSSchemaRegistryConstants.AWS_REGION) == null){
            throw new DataException("Target Region is not provided.");
        }

        sourceConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_REGION));
        targetConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, configs.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION));

        serializer   = new GlueSchemaRegistrySerializerImpl(credentialsProvider, new GlueSchemaRegistryConfiguration(targetConfigs));
        deserializer = new GlueSchemaRegistryDeserializerImpl(credentialsProvider, new GlueSchemaRegistryConfiguration(sourceConfigs));
    }

    @Override
    public byte[] fromConnectData(String topic, org.apache.kafka.connect.data.Schema schema, Object value) {
        if (value == null) return null;
        byte[] bytes = (byte[]) value;

        try {
            byte[] deserializedBytes = deserializer.getData(bytes);
            Schema deserializedSchema = deserializer.getSchema(bytes);
            //The registry is decided by the configuration in the target region , schema name is the same as the source region
            return serializer.encode(topic, deserializedSchema, deserializedBytes);

        } catch (SerializationException | AWSSchemaRegistryException e){
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
}