package com.amazonaws.services.schemaregistry.utils.json;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface ObjectMapperFactory {

    ObjectMapper newInstance(GlueSchemaRegistryConfiguration configs);
}
