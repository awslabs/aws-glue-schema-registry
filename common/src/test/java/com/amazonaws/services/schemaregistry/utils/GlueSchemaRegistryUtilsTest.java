package com.amazonaws.services.schemaregistry.utils;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GlueSchemaRegistryUtilsTest {
    private GlueSchemaRegistryUtils glueSchemaRegistryUtils;
    private final Map<String, Object> configs = new HashMap<>();

    @BeforeEach
    public void setup() {
        glueSchemaRegistryUtils = GlueSchemaRegistryUtils.getInstance();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "http://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, "json");
    }

    @Test
    public void testCheckIfPresentInMap_nullMap_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> glueSchemaRegistryUtils.checkIfPresentInMap(null, "test-key"));
    }

    @Test
    public void testCheckIfPresentInMap_nullKey_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> glueSchemaRegistryUtils.checkIfPresentInMap(configs, null));
    }

    @Test
    public void testConfigureSchemaNamingStrategy_configMapWithoutSchemaGenerationClass_succeeds() {
        assertDoesNotThrow(() -> glueSchemaRegistryUtils.configureSchemaNamingStrategy(configs));
    }

    @Test
    public void testConfigureSchemaNamingStrategy_configWithValidSchemaGenerationClass_succeeds() {
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS, "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy");
        assertDoesNotThrow(() -> glueSchemaRegistryUtils.configureSchemaNamingStrategy(configs));
     }

     @Test
    public void testConfigureSchemaNamingStrategy_configWithInvalidSchemaGenerationClass_returnsNull() {
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS, "com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtilsTest");
        assertNull(glueSchemaRegistryUtils.configureSchemaNamingStrategy(configs));
     }

    @Test
    public void testGetSchemaName_configWithoutSchemaName_schemaNameMatches() {
        String expectedSchemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString();
        assertEquals(expectedSchemaName, glueSchemaRegistryUtils.getSchemaName(configs));
    }

    @Test
    public void testGetSchemaName_configWithoutSchemaName_returnsNull() {
        configs.remove(AWSSchemaRegistryConstants.SCHEMA_NAME);
        assertNull(glueSchemaRegistryUtils.getSchemaName(configs));
    }

    @Test
    public void testGetDataFormat_configWithDataFormat_schemaNameMatches() {
        String expectedDataFormat = configs.get(AWSSchemaRegistryConstants.DATA_FORMAT).toString().toUpperCase();
        assertEquals(expectedDataFormat, glueSchemaRegistryUtils.getDataFormat(configs));
    }

    @Test
    public void testGetDataFormat_configWithoutDataFormat_returnsNull() {
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
        assertThrows(AWSSchemaRegistryException.class, () -> glueSchemaRegistryUtils.getDataFormat(configs));
    }

    @Test
    public void testUseCustomerProvidedStrategyMethod_validClassName_succeeds() throws NoSuchMethodException {
        Method useCustomerProvidedStrategyMethod = GlueSchemaRegistryUtils.class.getDeclaredMethod("useCustomerProvidedStrategy", String.class);
        useCustomerProvidedStrategyMethod.setAccessible(true);
        assertDoesNotThrow(() -> useCustomerProvidedStrategyMethod.invoke(glueSchemaRegistryUtils, "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy"));
    }

    @Test
    public void testUseCustomerProvidedStrategyMethod_invalidClassName_throwsException() throws NoSuchMethodException {
        Method useCustomerProvidedStrategyMethod = GlueSchemaRegistryUtils.class.getDeclaredMethod("useCustomerProvidedStrategy", String.class);
        useCustomerProvidedStrategyMethod.setAccessible(true);
        try {
            useCustomerProvidedStrategyMethod.invoke(glueSchemaRegistryUtils, "test");
        } catch(Exception e) {
            assertEquals(AWSSchemaRegistryException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testUseCustomerProvidedStrategyMethod_nullClassName_throwsException() throws NoSuchMethodException {
        Method useCustomerProvidedStrategyMethod = GlueSchemaRegistryUtils.class.getDeclaredMethod("useCustomerProvidedStrategy", String.class);
        useCustomerProvidedStrategyMethod.setAccessible(true);
        try {
            useCustomerProvidedStrategyMethod.invoke(glueSchemaRegistryUtils, null);
        } catch(Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void testInitializeStrategyStrategyMethod_validClassName_succeeds() throws NoSuchMethodException {
        Method initializeStrategyMethod = GlueSchemaRegistryUtils.class.getDeclaredMethod("initializeStrategy", String.class);
        initializeStrategyMethod.setAccessible(true);
        assertDoesNotThrow(() -> initializeStrategyMethod.invoke(glueSchemaRegistryUtils, "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy"));
    }

    @Test
    public void testInitializeStrategyStrategyMethod_nullClassName_throwsException() throws NoSuchMethodException {
        Method initializeStrategyMethod = GlueSchemaRegistryUtils.class.getDeclaredMethod("initializeStrategy", String.class);
        initializeStrategyMethod.setAccessible(true);
        try {
            initializeStrategyMethod.invoke(glueSchemaRegistryUtils, null);
        } catch(Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }
}