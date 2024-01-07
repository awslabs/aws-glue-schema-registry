package com.amazonaws.services.schemaregistry.utils.json;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * Unit tests for testing object mapper
 */
public class ObjectMapperUtilsTest {

    static GlueSchemaRegistryConfiguration testConfigs(Map<String, Object> map) {
        return new GlueSchemaRegistryConfiguration(testMap(map));
    }

    static Map<String, Object> testMap(Map<String, Object> map) {
        if (map == null) {
            map = new HashMap<>();
        }
        map.putIfAbsent(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        return map;
    }

    @Test
    void testCreate_succeeds() {
        GlueSchemaRegistryConfiguration cfg = testConfigs(null);

        ObjectMapper om = ObjectMapperUtils.create(cfg);
        assertNotNull(om);
    }

    @Test
    void testCreate_useJavaTimeModuleWithoutDeps_throwsException() {
        Map<String, Object> map = testMap(null);
        map.put(AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE, "com.fasterxml.jackson.datatype.jsr310.JavaTimeModule");
        GlueSchemaRegistryConfiguration cfg = testConfigs(map);
        assertThrows(AWSSchemaRegistryException.class, () -> ObjectMapperUtils.create(cfg));
    }

    @Test
    void testCreate_useJavaTimeModuleWithMockTimeModule_succeeds() {
        Map<String, Object> map = testMap(null);
        map.put(AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE, "com.fasterxml.jackson.datatype.jsr310.JavaTimeModule");

        GlueSchemaRegistryConfiguration spy = spy(testConfigs(map));
        doReturn(new SimpleModule()).when(spy).loadJavaTimeModule();

        ObjectMapper om = ObjectMapperUtils.create(spy);
        assertNotNull(om);
    }
}
