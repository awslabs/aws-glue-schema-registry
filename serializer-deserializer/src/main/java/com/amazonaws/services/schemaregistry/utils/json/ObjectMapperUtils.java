package com.amazonaws.services.schemaregistry.utils.json;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectMapperUtils {

    public static ObjectMapper create(GlueSchemaRegistryConfiguration configs) {
        try {
            Class<?> moduleClass = Class.forName(configs.getObjectMapperFactory());
            ObjectMapperFactory factory = (ObjectMapperFactory) moduleClass.getConstructor().newInstance();
            return factory.newInstance(configs);
        } catch (Exception e) {
            String message = String.format("Failed to instantiate ObjectMapperFactory: %s",
                    configs.getObjectMapperFactory());
            throw new AWSSchemaRegistryException(message, e);
        }
    }
}
