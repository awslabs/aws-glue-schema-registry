package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

import kotlinx.serialization.SerializationException;
import lombok.Getter;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CrossRegionReplicationMM2Converter implements Converter {
    @Getter
    private GlueSchemaRegistryDeserializationFacade deserializationFacade;
    @Getter
    private GlueSchemaRegistrySerializationFacade serializationFacade;
    @Getter
    private AwsCredentialsProvider credentialsProvider;
    @Getter
    private GlueSchemaRegistryDeserializerImpl deserializer;
    @Getter
    private GlueSchemaRegistrySerializerImpl serializer;
    @Getter
    private boolean isKey;

    /**
     * Constructor used by Kafka Connect user.
     */
    public CrossRegionReplicationMM2Converter(){};

    /**
     * Constructor accepting AWSCredentialsProvider.
     *
     * @param credentialsProvider AWSCredentialsProvider instance.
     */
    public CrossRegionReplicationMM2Converter(
            GlueSchemaRegistryDeserializationFacade deserializationFacade,
            GlueSchemaRegistrySerializationFacade serializationFacade,
            AwsCredentialsProvider credentialsProvider,
            GlueSchemaRegistryDeserializerImpl deserializerImpl,
            GlueSchemaRegistrySerializerImpl serializerImpl) {
        this.deserializationFacade = deserializationFacade;
        this.serializationFacade = serializationFacade;
        this.credentialsProvider = credentialsProvider;
        this.deserializer = deserializerImpl;
        this.serializer = serializerImpl;
    }

    /**
     * Configure the MM2 Schema Replication Converter.
     * @param configs configuration elements for the converter
     * @param isKey true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        new CrossRegionReplicationMM2ConverterConfig(configs);


        credentialsProvider = DefaultCredentialsProvider.builder().build();

        // Put the source and target regions into configurations respectively
        Map<String, Object> sourceConfigs = new HashMap<>(configs);
        Map<String, Object> targetConfigs = new HashMap<>(configs);

        sourceConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, configs.get(AWSSchemaRegistryConstants.AWS_SRC_REGION));
        targetConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, configs.get(AWSSchemaRegistryConstants.AWS_TGT_REGION));

        deserializationFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .credentialProvider(credentialsProvider)
                        .configs(sourceConfigs)
                        .build();

        serializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .credentialProvider(credentialsProvider)
                        .configs(targetConfigs)
                        .build();

        serializer = new GlueSchemaRegistrySerializerImpl(credentialsProvider, new GlueSchemaRegistryConfiguration(targetConfigs));

        deserializer = new GlueSchemaRegistryDeserializerImpl(credentialsProvider, new GlueSchemaRegistryConfiguration(sourceConfigs));


//        deserializationFacade = new GlueSchemaRegistryDeserializationFacade(new GlueSchemaRegistryConfiguration(sourceConfigs), credentialsProvider);
//
//        serializationFacade = new GlueSchemaRegistrySerializationFacade(credentialsProvider, null, new GlueSchemaRegistryConfiguration(targetConfigs), null, null);


    }

    @Override
    public byte[] fromConnectData(String topic, org.apache.kafka.connect.data.Schema schema, Object value) {
        byte[] bytes = (byte[]) value;
        if (value == null) return new byte[0];

        try {
            byte[] deserializedBytes = deserializer.getData(bytes);
            Schema deserializedSchema = deserializer.getSchema(bytes);

            byte[] encodedByte = serializer.encode(null, deserializedSchema, deserializedBytes);
            com.amazonaws.services.schemaregistry.common.Schema returnedSchema = getSchema(bytes);
            UUID uuid = registerSchema(returnedSchema);

            return encodedByte;
        } catch (SerializationException | AWSSchemaRegistryException e){
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization/deserialization error: ", e);
        }

    }

    @Override
    public SchemaAndValue toConnectData(String s, byte[] bytes) {
        return null;
    }

    /**
     * Retrieve schema from source region GSR using schema header of the serialized messages
     * @param data serialized message obtained from MM2
     * @return schema
     */
    public Schema getSchema(byte[] data){
        if (data == null) {
            throw new NullPointerException("Empty Data");
        }
            return deserializationFacade.getSchema(data);

    }

    /**
     * Register schema in the target region GSR
     * @param schema schema obtained from the source region GSR
     * @return schema version ID of the registered schema
     */
    public  UUID registerSchema(com.amazonaws.services.schemaregistry.common.Schema schema){
        try{
            String schemaDefinition = schema.getSchemaDefinition();
            String schemaName = schema.getSchemaName();
            String dataFormat = schema.getDataFormat();
            AWSSerializerInput input = new AWSSerializerInput(schemaDefinition, schemaName, dataFormat, null);
            return serializationFacade.getOrRegisterSchemaVersion(input);
        } catch (Exception e){
            throw new DataException("Schema can't be register");
        }
    }
}
