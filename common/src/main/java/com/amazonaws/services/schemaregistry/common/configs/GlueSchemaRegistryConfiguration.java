/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.common.configs;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.EnumUtils;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.glue.model.Compatibility;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Glue Schema Registry Configuration entries.
 */
@Slf4j
@Data
public class GlueSchemaRegistryConfiguration {
    private static final String DELIMITER = "-";
    private AWSSchemaRegistryConstants.COMPRESSION compressionType = AWSSchemaRegistryConstants.COMPRESSION.NONE;
    private String endPoint;
    private String region;
    // TODO: Remove configs that are not useful non replication use-cases
    // https://github.com/awslabs/aws-glue-schema-registry/issues/292
    private String sourceEndPoint;
    private String sourceRegion;
    private String targetEndPoint;
    private String targetRegion;
    private long timeToLiveMillis = 24 * 60 * 60 * 1000L;
    private int cacheSize = 200;
    private AvroRecordType avroRecordType;
    private ProtobufMessageType protobufMessageType;
    private String registryName;
    private String sourceRegistryName;
    private String targetRegistryName;
    private int replicateSchemaVersionCount;
    private Compatibility compatibilitySetting;
    private String description;
    private boolean schemaAutoRegistrationEnabled = false;
    private Map<String, String> tags = new HashMap<>();
    private Map<String, String> metadata;
    private String secondaryDeserializer;
    private URI proxyUrl;

    /**
     * Name of the application using the serializer/deserializer.
     * Ex: Kafka, KafkaConnect, KPL etc.
     */
    private String userAgentApp = "default";

    private List<SerializationFeature> jacksonSerializationFeatures;
    private List<DeserializationFeature> jacksonDeserializationFeatures;

    public GlueSchemaRegistryConfiguration(String region) {
        Map<String, Object> config = new HashMap<>();
        config.put(AWSSchemaRegistryConstants.AWS_REGION, region);
        buildConfigs(config);
    }

    public GlueSchemaRegistryConfiguration(Map<String, ?> configs) {
        buildConfigs(configs);
    }

    public GlueSchemaRegistryConfiguration(Properties properties) {
        buildConfigs(getMapFromPropertiesFile(properties));
    }

    private void buildConfigs(Map<String, ?> configs) {
        buildSchemaRegistryConfigs(configs);
        buildCacheConfigs(configs);
    }

    private void buildSchemaRegistryConfigs(Map<String, ?> configs) {
        validateAndSetAWSRegion(configs);
        validateAndSetAWSSourceRegion(configs);
        validateAndSetAWSTargetRegion(configs);
        validateAndSetAWSEndpoint(configs);
        validateAndSetAWSSourceEndpoint(configs);
        validateAndSetAWSTargetEndpoint(configs);
        validateAndSetRegistryName(configs);
        validateAndSetSourceRegistryName(configs);
        validateAndSetTargetRegistryName(configs);
        validateAndSetReplicateSchemaVersionCount(configs);
        validateAndSetDescription(configs);
        validateAndSetAvroRecordType(configs);
        validateAndSetProtobufMessageType(configs);
        validateAndSetCompatibility(configs);
        validateAndSetCompressionType(configs);
        validateAndSetSchemaAutoRegistrationSetting(configs);
        validateAndSetJacksonSerializationFeatures(configs);
        validateAndSetJacksonDeserializationFeatures(configs);
        validateAndSetTags(configs);
        validateAndSetMetadata(configs);
        validateAndSetUserAgent(configs);
        validateAndSetSecondaryDeserializer(configs);
        validateAndSetProxyUrl(configs);
    }

    private void validateAndSetSecondaryDeserializer(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER)) {
            Object secondaryDeserializer = configs.get(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER);
            if (secondaryDeserializer instanceof String) {
                this.secondaryDeserializer = (String) secondaryDeserializer;
            } else if (secondaryDeserializer instanceof Class) {
                this.secondaryDeserializer = ((Class) secondaryDeserializer).getName();
            } else {
                throw new AWSSchemaRegistryException("Invalid secondary de-serializer configuration");
            }
        }
    }

    private void buildCacheConfigs(Map<String, ?> configs) {
        validateAndSetCacheSize(configs);
        validateAndSetCacheTTL(configs);
    }

    private void validateAndSetUserAgent(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.USER_AGENT_APP)) {
            this.userAgentApp = (String) configs.get(AWSSchemaRegistryConstants.USER_AGENT_APP);
        }
    }

    private void validateAndSetCompressionType(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.COMPRESSION_TYPE) && validateCompressionType(
                (String) configs.get(AWSSchemaRegistryConstants.COMPRESSION_TYPE))) {
            this.compressionType = AWSSchemaRegistryConstants.COMPRESSION.valueOf(
                    ((String) configs.get(AWSSchemaRegistryConstants.COMPRESSION_TYPE)).toUpperCase());
        }
    }

    private boolean validateCompressionType(String compressionType) {
        if (!EnumUtils.isValidEnum(AWSSchemaRegistryConstants.COMPRESSION.class, compressionType.toUpperCase())) {
            String errorMessage =
                    String.format("Invalid Compression type : %s, Accepted values are : %s", compressionType,
                                  AWSSchemaRegistryConstants.COMPRESSION.values());
            throw new AWSSchemaRegistryException(errorMessage);
        }
        return true;
    }

    private void validateAndSetAWSRegion(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_REGION)) {
            this.region = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_REGION));
        } else {
            try {
                this.region = DefaultAwsRegionProviderChain
                    .builder()
                    .build()
                    .getRegion()
                    .id();
            } catch (SdkClientException ex) {
                throw new AWSSchemaRegistryException("Region is not defined in the properties", ex);
            }
        }
    }

    private void validateAndSetAWSSourceRegion(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_SOURCE_REGION)) {
            this.sourceRegion = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_SOURCE_REGION));
        }
    }

    private void validateAndSetAWSTargetRegion(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_TARGET_REGION)) {
            this.targetRegion = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION));
            this.region = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_TARGET_REGION));
        } else {
            this.targetRegion = this.region;
        }
    }

    private void validateAndSetCompatibility(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.COMPATIBILITY_SETTING)) {
            this.compatibilitySetting = Compatibility.fromValue(
                    String.valueOf(configs.get(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING))
                            .toUpperCase());

            if (this.compatibilitySetting == null
                || this.compatibilitySetting == Compatibility.UNKNOWN_TO_SDK_VERSION) {
                String errorMessage = String.format("Invalid compatibility setting : %s, Accepted values are : %s",
                                                    configs.get(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING),
                                                    Compatibility.knownValues());
                throw new AWSSchemaRegistryException(errorMessage);
            }
        } else {
            this.compatibilitySetting = AWSSchemaRegistryConstants.DEFAULT_COMPATIBILITY_SETTING;
        }
    }

    private void validateAndSetRegistryName(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.REGISTRY_NAME)) {
            this.registryName = String.valueOf(configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME));
        } else {
            this.registryName = AWSSchemaRegistryConstants.DEFAULT_REGISTRY_NAME;
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
            this.registryName = String.valueOf(configs.get(AWSSchemaRegistryConstants.TARGET_REGISTRY_NAME));
        } else {
            this.targetRegistryName = this.registryName;
        }
    }

    private void validateAndSetReplicateSchemaVersionCount(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.REPLICATE_SCHEMA_VERSION_COUNT)) {
            this.replicateSchemaVersionCount = Integer.valueOf(configs.get(AWSSchemaRegistryConstants.REPLICATE_SCHEMA_VERSION_COUNT).toString());
        } else {
            this.replicateSchemaVersionCount = AWSSchemaRegistryConstants.DEFAULT_REPLICATE_SCHEMA_VERSION_COUNT;
        }
    }

    private void validateAndSetAWSEndpoint(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_ENDPOINT)) {
            this.endPoint = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_ENDPOINT));
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
            this.endPoint = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_TARGET_ENDPOINT));
        } else {
            this.targetEndPoint = String.valueOf(configs.get(AWSSchemaRegistryConstants.AWS_ENDPOINT));
        }
    }

    private void validateAndSetProxyUrl(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.PROXY_URL)) {
                String value = (String) configs.get(AWSSchemaRegistryConstants.PROXY_URL);
                try {
                        this.proxyUrl = URI.create(value);
                } catch (IllegalArgumentException e) {
                        String message = String.format("Proxy URL property is not a valid URL: %s", value);
                        throw new AWSSchemaRegistryException(message, e);
                }
        }
    }

    private void validateAndSetDescription(Map<String, ?> configs) throws AWSSchemaRegistryException {
        if (isPresent(configs, AWSSchemaRegistryConstants.DESCRIPTION)) {
            this.description = String.valueOf(configs.get(AWSSchemaRegistryConstants.DESCRIPTION));
        } else {
            this.description = buildDescriptionFromProperties();
        }
    }

    private void validateAndSetCacheSize(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.CACHE_SIZE)) {
            String value = (String) configs.get(AWSSchemaRegistryConstants.CACHE_SIZE);

            try {
                this.cacheSize = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                String message = String.format("Cache size property is not a valid size : %s", value);
                throw new AWSSchemaRegistryException(message, e);
            }
        } else {
            log.info("Cache Size is not found, using default {}", cacheSize);
        }
    }

    private void validateAndSetCacheTTL(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS)) {
            String value = (String) configs.get(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS);
            try {
                this.timeToLiveMillis = Long.parseLong(value);
            } catch (NumberFormatException e) {
                String message = String.format("Time to live cache property is not a valid time : %s", value);
                throw new AWSSchemaRegistryException(message, e);
            }
        } else {
            log.info("Cache Time to live is not found, using default {}", timeToLiveMillis);
        }
    }

    private void validateAndSetAvroRecordType(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AVRO_RECORD_TYPE)) {
            this.avroRecordType =
                    AvroRecordType.valueOf((String) configs.get(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE));
        }
    }

    private void validateAndSetProtobufMessageType(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE)) {
            this.protobufMessageType =
                    ProtobufMessageType.valueOf((String) configs.get(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE));
        }
    }

    private void validateAndSetSchemaAutoRegistrationSetting(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING)) {
            this.schemaAutoRegistrationEnabled = Boolean.parseBoolean(
                    configs.get(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING)
                            .toString());
        } else {
            log.info("schemaAutoRegistrationEnabled is not defined in the properties. Using the default value {}",
                     schemaAutoRegistrationEnabled);
        }
    }

    private void validateAndSetTags(Map<String, ?> configs) throws AWSSchemaRegistryException {
        if (isPresent(configs, AWSSchemaRegistryConstants.TAGS)) {
            Map<String, String> tagsMap;
            if (configs.get(AWSSchemaRegistryConstants.TAGS) instanceof HashMap) {
                tagsMap = (Map<String, String>) configs.get(AWSSchemaRegistryConstants.TAGS);
                this.tags = tagsMap;
            } else {
                throw new AWSSchemaRegistryException(AWSSchemaRegistryConstants.TAGS_CONFIG_NOT_HASHMAP_MSG);
            }
        } else {
            log.info("Tags value is not defined in the properties. No tags are assigned");
        }
    }

    private void validateAndSetMetadata(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.METADATA)) {
            if (configs.get(AWSSchemaRegistryConstants.METADATA) instanceof HashMap) {
                Map<String, String> map = (Map<String, String>) configs.get(AWSSchemaRegistryConstants.METADATA);
                this.metadata = map;
            } else {
                throw new AWSSchemaRegistryException("The metadata instance is not a hash map");
            }
        }
    }

    private void validateAndSetJacksonSerializationFeatures(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES)) {
            if (configs.get(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES) instanceof List) {
                List<String> serialzationFeatures =
                        (List<String>) configs.get(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES);
                this.jacksonSerializationFeatures = serialzationFeatures.stream()
                        .map(sf -> SerializationFeature.valueOf(sf))
                        .collect(Collectors.toList());
            } else {
                throw new AWSSchemaRegistryException("Jackson Serialization features should be a list");
            }
        }
    }

    private void validateAndSetJacksonDeserializationFeatures(Map<String, ?> configs) {
        if (isPresent(configs, AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES)) {
            if (configs.get(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES) instanceof List) {
                List<String> deserialzationFeatures =
                        (List<String>) configs.get(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES);
                this.jacksonDeserializationFeatures = deserialzationFeatures.stream()
                        .map(dsf -> DeserializationFeature.valueOf(dsf))
                        .collect(Collectors.toList());
            } else {
                throw new AWSSchemaRegistryException("Jackson Deserialization features should be a list");
            }
        }
    }

    private boolean isPresent(Map<String, ?> configs,
                              String key) {
        if (!GlueSchemaRegistryUtils.getInstance()
                .checkIfPresentInMap(configs, key)) {
            log.info("{} key is not present in the configs", key);
            return false;
        }
        return true;
    }

    private Map<String, ?> getMapFromPropertiesFile(Properties properties) {
        return new HashMap<>(properties.entrySet()
                                     .stream()
                                     .collect(Collectors.toMap(e -> e.getKey()
                                             .toString(), e -> e.getValue())));
    }

    private String buildDescriptionFromProperties() throws AWSSchemaRegistryException {
        StringBuilder message = new StringBuilder();
        message.append("DEFAULT-DESCRIPTION")
                .append(DELIMITER)
                .append(region)
                .append(DELIMITER)
                .append(registryName);

        return message.toString();
    }
}
