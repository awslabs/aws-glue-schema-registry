package com.amazonaws.services.schemaregistry.integrationtests.kafka;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.json.ObjectMapperUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectMapperDateTimeSupportTest {

    @Test
    void testCreate_useJavaTimeModule_succeeds() {
        Map<String, Object> map = new HashMap<>();
        map.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        map.put(AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE, "com.fasterxml.jackson.datatype.jsr310.JavaTimeModule");
        GlueSchemaRegistryConfiguration cfg = new GlueSchemaRegistryConfiguration(map);

        ObjectMapper om = ObjectMapperUtils.create(cfg);
        assertNotNull(cfg.loadJavaTimeModule());
        assertNotNull(om);
    }

    @Test
    void testCreate_noJavaTimeModule_failsSerializingJava8DateTime() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        map.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        GlueSchemaRegistryConfiguration cfg = new GlueSchemaRegistryConfiguration(map);

        ObjectMapper om = ObjectMapperUtils.create(cfg);

        MyTestObject myObj = new MyTestObject("obj1", LocalDateTime.now());

        assertThrows(InvalidDefinitionException.class, () -> om.writeValueAsString(myObj));
    }

    @Test
    void testCreate_withJavaTimeModule_succeedsSerializingAndDeserializingJava8DateTime() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        map.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        map.put(AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE, "com.fasterxml.jackson.datatype.jsr310.JavaTimeModule");
        GlueSchemaRegistryConfiguration cfg = new GlueSchemaRegistryConfiguration(map);

        ObjectMapper om = ObjectMapperUtils.create(cfg);

        MyTestObject expectedObj = new MyTestObject("obj1", LocalDateTime.now());
        String jsonStr = om.writeValueAsString(expectedObj);

        MyTestObject actualObj = om.readValue(jsonStr, MyTestObject.class);
        assertNotNull(actualObj);
        assertEquals(expectedObj.getCreated(), actualObj.getCreated());
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class MyTestObject {
        String name;
        LocalDateTime created;
    }
}
