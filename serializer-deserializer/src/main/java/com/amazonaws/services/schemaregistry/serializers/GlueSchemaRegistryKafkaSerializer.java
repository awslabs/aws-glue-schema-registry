package com.amazonaws.services.schemaregistry.serializers;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;
import java.util.UUID;

/**
 * Glue Schema Registry Serializer to be used with Kafka Producers.
 */
@Slf4j
@Data
public class GlueSchemaRegistryKafkaSerializer implements Serializer<Object> {
    private final AwsCredentialsProvider credentialProvider;
    private final UUID schemaVersionId;
    private String dataFormat;
    private GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade;
    private String schemaName;
    private AWSSchemaNamingStrategy schemaNamingStrategy;
    private boolean isKey;

    /**
     * Constructor used by Kafka producer when passing as the property.
     */
    public GlueSchemaRegistryKafkaSerializer() {
        this(DefaultCredentialsProvider.builder()
                     .build(), null, null);
    }

    public GlueSchemaRegistryKafkaSerializer(Map<String, ?> configs) {
        this(DefaultCredentialsProvider.builder()
                     .build(), null, configs);
    }

    public GlueSchemaRegistryKafkaSerializer(AwsCredentialsProvider credentialProvider,
                                             Map<String, ?> configs) {
        this(credentialProvider, null, configs);
    }

    public GlueSchemaRegistryKafkaSerializer(@NonNull Map<String, ?> configs,
                                             UUID schemaVersionId) {
        this(DefaultCredentialsProvider.builder()
                     .build(), schemaVersionId, configs);
    }

    public GlueSchemaRegistryKafkaSerializer(AwsCredentialsProvider credentialProvider,
                                             UUID schemaVersionId,
                                             Map<String, ?> configs) {
        this.credentialProvider = (credentialProvider == null) ? DefaultCredentialsProvider.builder()
                .build() : credentialProvider;
        this.schemaVersionId = schemaVersionId;
        if (configs != null) {
            configure(configs, false);
        }
    }

    @Override
    public void configure(@NonNull Map<String, ?> configs,
                          boolean isKey) {
        log.info("Configuring Glue Schema Registry Client using these properties: {}", configs);
        schemaName = GlueSchemaRegistryUtils.getInstance()
                .getSchemaName(configs);
        this.isKey = isKey;

        dataFormat = GlueSchemaRegistryUtils.getInstance()
                .getDataFormat(configs);

        if (schemaName == null) {
            schemaNamingStrategy = GlueSchemaRegistryUtils.getInstance()
                    .configureSchemaNamingStrategy(configs);
        }

        if (glueSchemaRegistrySerializationFacade == null) {
            glueSchemaRegistrySerializationFacade = GlueSchemaRegistrySerializationFacade.builder()
                    .configs(configs)
                    .credentialProvider(credentialProvider)
                    .build();
        }
    }

    @Override
    public byte[] serialize(String topic,
                            Object data) {
        byte[] result = null;

        if (null == data) {
            return null;
        }

        UUID schemaVersionIdFromRegistry = null;
        if (this.schemaVersionId == null) {
            log.debug("Schema Version Id is null. Trying to register the schema.");
            schemaVersionIdFromRegistry =
                    glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(prepareInput(data, topic, isKey));
        } else {
            schemaVersionIdFromRegistry = this.schemaVersionId;
        }

        if (schemaVersionIdFromRegistry != null) {
            log.debug("Schema Version Id received from the from schema registry: {}", schemaVersionIdFromRegistry);
            result = glueSchemaRegistrySerializationFacade.serialize(DataFormat.fromValue(dataFormat), data,
                                                                     schemaVersionIdFromRegistry);
        }

        return result;
    }

    @Override
    public void close() {
    }

    /**
     * Provide implementation of AWSSchemaNamingStrategy via a dynamic configuration.
     * OR
     * Provide the schema name in the dynamic configuration.
     * OR
     * Client will generate the one using the AWSSchemaNamingStrategyDefaultImpl
     *
     * @param topic Name of the topic
     * @param data  data or record
     * @param isKey flag that indicates if the record is a key or not
     * @return schemaName.
     */
    private String getSchemaName(String topic,
                                 Object data,
                                 Boolean isKey) {
        if (schemaName == null) {
            return schemaNamingStrategy.getSchemaName(topic, data, isKey);
        }

        return schemaName;
    }

    private AWSSerializerInput prepareInput(@NonNull Object data,
                                            String topic,
                                            Boolean isKey) {
        String schemaDefinition =
                glueSchemaRegistrySerializationFacade.getSchemaDefinition(DataFormat.fromValue(dataFormat), data);

        return AWSSerializerInput.builder()
                .schemaDefinition(schemaDefinition)
                .schemaName(getSchemaName(topic, data, isKey))
                .transportName(topic)
                .dataFormat(dataFormat)
                .build();
    }
}
