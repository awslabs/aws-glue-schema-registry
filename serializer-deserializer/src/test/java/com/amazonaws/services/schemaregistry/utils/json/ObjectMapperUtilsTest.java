package com.amazonaws.services.schemaregistry.utils.json;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for testing object mapper
 */
public class ObjectMapperUtilsTest {

    static GlueSchemaRegistryConfiguration testConfigs(Map<String, Object> configs) {
        configs.putIfAbsent(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        return new GlueSchemaRegistryConfiguration(configs);
    }

    @Test
    void testCreate_succeeds() {
        GlueSchemaRegistryConfiguration cfg = testConfigs(new HashMap<String, Object>());

        ObjectMapper om = ObjectMapperUtils.create(cfg);
        assertNotNull(om);
    }
}
