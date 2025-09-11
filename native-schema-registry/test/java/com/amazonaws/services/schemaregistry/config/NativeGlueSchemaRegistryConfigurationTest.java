package com.amazonaws.services.schemaregistry.config;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class NativeGlueSchemaRegistryConfigurationTest {

    private static final String REGION_KEY = "region";
    private static final String ROLE_TO_ASSUME_KEY = "roleToAssume";
    private static final String ROLE_SESSION_NAME_KEY = "roleSessionName";
    
    private static final String REGION_VALUE = "us-east-1";
    private static final String ROLE_ARN_VALUE = "arn:aws:iam::123456789012:role/TestRole";
    private static final String DEFAULT_SESSION_NAME = "native-glue-schema-registry";
    private static final String CUSTOM_SESSION_NAME = "custom-session-name";

    @Test
    void testDefaultSessionNameSetWhenRoleConfiguredButSessionNameNot() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(REGION_KEY, REGION_VALUE);
        configs.put(ROLE_TO_ASSUME_KEY, ROLE_ARN_VALUE);
        
        NativeGlueSchemaRegistryConfiguration config = new NativeGlueSchemaRegistryConfiguration(configs);
        
        assertEquals(ROLE_ARN_VALUE, config.getRoleToAssume());
        assertEquals(DEFAULT_SESSION_NAME, config.getRoleSessionName());
    }

    @Test
    void testCustomSessionNameOverridesDefault() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(REGION_KEY, REGION_VALUE);
        configs.put(ROLE_TO_ASSUME_KEY, ROLE_ARN_VALUE);
        configs.put(ROLE_SESSION_NAME_KEY, CUSTOM_SESSION_NAME);
        
        NativeGlueSchemaRegistryConfiguration config = new NativeGlueSchemaRegistryConfiguration(configs);
        
        assertEquals(ROLE_ARN_VALUE, config.getRoleToAssume());
        assertEquals(CUSTOM_SESSION_NAME, config.getRoleSessionName());
    }

    @Test
    void testStsAssumeRoleCredentialsProviderReturnedWhenRoleConfigured() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(REGION_KEY, REGION_VALUE);
        configs.put(ROLE_TO_ASSUME_KEY, ROLE_ARN_VALUE);
        
        NativeGlueSchemaRegistryConfiguration config = new NativeGlueSchemaRegistryConfiguration(configs);
        AwsCredentialsProvider credentialsProvider = config.getAwsCredentialsProvider();
        
        assertTrue(credentialsProvider instanceof StsAssumeRoleCredentialsProvider, 
            "Expected StsAssumeRoleCredentialsProvider when role is configured");
    }

    @Test
    void testDefaultCredentialsProviderReturnedWhenNoRoleConfigured() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(REGION_KEY, REGION_VALUE);
        
        NativeGlueSchemaRegistryConfiguration config = new NativeGlueSchemaRegistryConfiguration(configs);
        AwsCredentialsProvider credentialsProvider = config.getAwsCredentialsProvider();
        
        assertTrue(credentialsProvider instanceof DefaultCredentialsProvider, 
            "Expected DefaultCredentialsProvider when no role is configured");
    }

    @Test
    void testPropertiesConstructorSetsDefaults() {
        Properties properties = new Properties();
        properties.setProperty(REGION_KEY, REGION_VALUE);
        properties.setProperty(ROLE_TO_ASSUME_KEY, ROLE_ARN_VALUE);
        
        NativeGlueSchemaRegistryConfiguration config = new NativeGlueSchemaRegistryConfiguration(properties);
        
        assertEquals(ROLE_ARN_VALUE, config.getRoleToAssume());
        assertEquals(DEFAULT_SESSION_NAME, config.getRoleSessionName());
    }
}
