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

public class AWSSchemaRegistryUtilsTest {
    private AWSSchemaRegistryUtils awsSchemaRegistryUtils;
    private final Map<String, Object> configs = new HashMap<>();

    @BeforeEach
    public void setup() {
        awsSchemaRegistryUtils = AWSSchemaRegistryUtils.getInstance();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "http://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
    }

    @Test
    public void testCheckIfPresentInMap_nullMap_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> awsSchemaRegistryUtils.checkIfPresentInMap(null, "test-key"));
    }

    @Test
    public void testCheckIfPresentInMap_nullKey_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> awsSchemaRegistryUtils.checkIfPresentInMap(configs, null));
    }

    @Test
    public void testConfigureSchemaNamingStrategy_configMapWithoutSchemaGenerationClass_succeeds() {
        assertDoesNotThrow(() -> awsSchemaRegistryUtils.configureSchemaNamingStrategy(configs));
    }

    @Test
    public void testConfigureSchemaNamingStrategy_configWithValidSchemaGenerationClass_succeeds() {
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS, "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy");
        assertDoesNotThrow(() -> awsSchemaRegistryUtils.configureSchemaNamingStrategy(configs));
     }

     @Test
    public void testConfigureSchemaNamingStrategy_configWithInvalidSchemaGenerationClass_returnsNull() {
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS, "com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryUtilsTest");
        assertNull(awsSchemaRegistryUtils.configureSchemaNamingStrategy(configs));
     }

    @Test
    public void testGetSchemaName_configWithoutSchemaName_schemaNameMatches() {
        String expectedSchemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString();
        assertEquals(expectedSchemaName, awsSchemaRegistryUtils.getSchemaName(configs));
    }

    @Test
    public void testGetSchemaName_configWithoutSchemaName_returnsNull() {
        configs.remove(AWSSchemaRegistryConstants.SCHEMA_NAME);
        assertNull(awsSchemaRegistryUtils.getSchemaName(configs));
    }

    @Test
    public void testUseCustomerProvidedStrategyMethod_validClassName_succeeds() throws NoSuchMethodException {
        Method useCustomerProvidedStrategyMethod = AWSSchemaRegistryUtils.class.getDeclaredMethod("useCustomerProvidedStrategy", String.class);
        useCustomerProvidedStrategyMethod.setAccessible(true);
        assertDoesNotThrow(() -> useCustomerProvidedStrategyMethod.invoke(awsSchemaRegistryUtils,  "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy"));
    }

    @Test
    public void testUseCustomerProvidedStrategyMethod_invalidClassName_throwsException() throws NoSuchMethodException {
        Method useCustomerProvidedStrategyMethod = AWSSchemaRegistryUtils.class.getDeclaredMethod("useCustomerProvidedStrategy", String.class);
        useCustomerProvidedStrategyMethod.setAccessible(true);
        try {
            useCustomerProvidedStrategyMethod.invoke(awsSchemaRegistryUtils,  "test");
        } catch(Exception e) {
            assertEquals(AWSSchemaRegistryException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testUseCustomerProvidedStrategyMethod_nullClassName_throwsException() throws NoSuchMethodException {
        Method useCustomerProvidedStrategyMethod = AWSSchemaRegistryUtils.class.getDeclaredMethod("useCustomerProvidedStrategy", String.class);
        useCustomerProvidedStrategyMethod.setAccessible(true);
        try {
            useCustomerProvidedStrategyMethod.invoke(awsSchemaRegistryUtils,  null);
        } catch(Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void testInitializeStrategyStrategyMethod_validClassName_succeeds() throws NoSuchMethodException {
        Method initializeStrategyMethod = AWSSchemaRegistryUtils.class.getDeclaredMethod("initializeStrategy", String.class);
        initializeStrategyMethod.setAccessible(true);
        assertDoesNotThrow(() -> initializeStrategyMethod.invoke(awsSchemaRegistryUtils,  "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy"));
    }

    @Test
    public void testInitializeStrategyStrategyMethod_nullClassName_throwsException() throws NoSuchMethodException {
        Method initializeStrategyMethod = AWSSchemaRegistryUtils.class.getDeclaredMethod("initializeStrategy", String.class);
        initializeStrategyMethod.setAccessible(true);
        try {
            initializeStrategyMethod.invoke(awsSchemaRegistryUtils,  null);
        } catch(Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }
}