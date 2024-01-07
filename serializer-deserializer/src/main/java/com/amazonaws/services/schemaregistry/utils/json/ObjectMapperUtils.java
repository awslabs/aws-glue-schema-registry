package com.amazonaws.services.schemaregistry.utils.json;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.collections4.CollectionUtils;

public class ObjectMapperUtils {

    public static ObjectMapper create(GlueSchemaRegistryConfiguration configs) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

        if (configs != null) {
            if (!CollectionUtils.isEmpty(configs.getJacksonSerializationFeatures())) {
                configs.getJacksonSerializationFeatures()
                        .forEach(objectMapper::enable);
            }
            if (!CollectionUtils.isEmpty(configs.getJacksonDeserializationFeatures())) {
                configs.getJacksonDeserializationFeatures()
                        .forEach(objectMapper::enable);
            }
            if (configs.getJavaTimeModuleClass() != null) {
                objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                objectMapper.registerModule(configs.loadJavaTimeModule());
            }
        }

        return objectMapper;
    }
}
