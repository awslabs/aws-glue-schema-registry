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
package com.amazonaws.services.schemaregistry.deserializers.avro;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.SecondaryDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Unit tests for testing Kafka specific de-serializer.
 */
@ExtendWith(MockitoExtension.class)
public class AWSKafkaAvroDeserializerTest {
    private Map<String, Object> configs = new HashMap<>();
    @Mock
    private AwsCredentialsProvider mockCredProvider;

    @BeforeEach
    public void setup() {
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT,
                "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, "NONE");
    }

    /**
     * Test AWSKafkaDeserializer instantiation.
     */
    @Test
    public void test_Create() {
        // Test create with empty constructor
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer1 = new AWSKafkaAvroDeserializer();
        assertNotNull(awsKafkaAvroDeserializer1.getCredentialProvider());

        // Test create with AWSCredentialsProvider constructor
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer2 = new AWSKafkaAvroDeserializer(this.mockCredProvider,
                configs);
        assertNotNull(awsKafkaAvroDeserializer2.getCredentialProvider());
    }

    @Test
    public void test_Create_With_Aws_Deserializer() {
        // Test create with empty constructor
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(this.mockCredProvider, null);
        awsKafkaAvroDeserializer.setGlueSchemaRegistryDeserializationFacade(
                GlueSchemaRegistryDeserializationFacade.builder().credentialProvider(this.mockCredProvider).configs(configs).build());

        assertNotNull(awsKafkaAvroDeserializer.getCredentialProvider());
        assertNotNull(awsKafkaAvroDeserializer.getGlueSchemaRegistryDeserializationFacade());
    }

    /**
     * Test AWSKafkaDeserializer configure method for empty configuration.
     */
    @Test
    public void test_Configure_Empty_Config() {
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer();
        assertNotNull(awsKafkaAvroDeserializer.getCredentialProvider());

        Map<String, Object> configs = new HashMap<>();
        assertThrows(IllegalArgumentException.class, () -> awsKafkaAvroDeserializer.configure(configs, false));
    }

    /**
     * Test AWSKafkaDeserializer deserialize method by mocking the dependency.
     */
    @Test
    public void test_Deserialize_Null_Input() {
        // Mock the dependency
        GlueSchemaRegistryDeserializationFacade
                glueSchemaRegistryDeserializationFacade = mock(GlueSchemaRegistryDeserializationFacade.class);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(this.mockCredProvider, null);
        awsKafkaAvroDeserializer.setGlueSchemaRegistryDeserializationFacade(glueSchemaRegistryDeserializationFacade);

        Object result = awsKafkaAvroDeserializer.deserialize("TestTopic", null);
        assertNull(result);
    }

    /**
     * Test AWSKafkaDeserializer deserialize method by mocking the dependency.
     */
    @Test
    public void test_Deserialize() {
        Object expectedObject = new Object();
        // Mock the dependency
        GlueSchemaRegistryDeserializationFacade
                glueSchemaRegistryDeserializationFacade = mock(GlueSchemaRegistryDeserializationFacade.class);
        when(glueSchemaRegistryDeserializationFacade.deserialize(Mockito.any(AWSDeserializerInput.class))).thenReturn(expectedObject);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(this.mockCredProvider, null);
        awsKafkaAvroDeserializer.setGlueSchemaRegistryDeserializationFacade(glueSchemaRegistryDeserializationFacade);

        Object deserializedObject = awsKafkaAvroDeserializer.deserialize("TestTopic",
                new byte[] { AWSSchemaRegistryConstants.HEADER_VERSION_BYTE });
        assertEquals(expectedObject, deserializedObject);
    }

    /**
     * Tests invoking shutdown invokes the internal AWSDeserializer.close method.
     */
    @Test
    public void testClose_callInternalAWSDeserializer_succeeds() {
        GlueSchemaRegistryDeserializationFacade
                glueSchemaRegistryDeserializationFacade = mock(GlueSchemaRegistryDeserializationFacade.class);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(this.mockCredProvider, null);
        awsKafkaAvroDeserializer.setGlueSchemaRegistryDeserializationFacade(glueSchemaRegistryDeserializationFacade);

        Mockito.verify(glueSchemaRegistryDeserializationFacade, Mockito.atMost(1)).close();
    }

    /**
     * Test AWSKafkaDeserializer configure method for null pointer exception by passing null config
     */
    @Test
    public void testConfigure_nullConfig_throwsException() {
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer();
        assertNotNull(awsKafkaAvroDeserializer.getCredentialProvider());

        assertThrows((IllegalArgumentException.class), () -> awsKafkaAvroDeserializer.configure(null, false));
    }

    /**
     * Test AWSKafkaDeserializer configure method for positive scenario by passing valid config.
     */
    @Test
    public void testConfigure_validConfig_throwsException() {
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer();
        assertNotNull(awsKafkaAvroDeserializer.getCredentialProvider());

        assertDoesNotThrow(() -> awsKafkaAvroDeserializer.configure(configs, false));
    }

    /**
     * Test AWSKafkaAvroDeserializer constructor for null pointer exception by passing null config.
     */
    @Test
    public void testConstructor_nullConfig_throwsException() {
        assertThrows((IllegalArgumentException.class), () -> new AWSKafkaAvroDeserializer(null));
    }

    /**
     * Tests invoking close method.
     */
    @Test
    public void testClose_succeeds() {
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer();
        awsKafkaAvroDeserializer.configure(configs, false);

        assertDoesNotThrow(() -> awsKafkaAvroDeserializer.close());
    }

    /**
     * Test routing logic with mocked secondary deserializer.
     * Verifies that Confluent-formatted data (0x3D header) routes to secondary deserializer.
     */
    @Test
    public void testRoutingLogic_ConfluentData_ShouldRouteToSecondary() {
        // Confluent data pattern: starts with '=' (0x3D), 6 bytes total
        byte[] confluentData = {0x3D, 0x44, 0x20, 0x20, 0x3E, 0x44}; // "=D  >D"
        
        // Create deserializer with mocked components
        AWSKafkaAvroDeserializer deserializer = new AWSKafkaAvroDeserializer(mockCredProvider, null);
        
        // Mock the primary deserializer facade (should NOT be called)
        GlueSchemaRegistryDeserializationFacade mockFacade = mock(GlueSchemaRegistryDeserializationFacade.class);
        deserializer.setGlueSchemaRegistryDeserializationFacade(mockFacade);
        
        // Mock the secondary deserializer (should be called)
        SecondaryDeserializer mockSecondaryDeserializer = mock(SecondaryDeserializer.class);
        when(mockSecondaryDeserializer.validate(Mockito.any())).thenReturn(true);
        when(mockSecondaryDeserializer.deserialize("test-topic", confluentData)).thenReturn("secondary-result");
        
        // Use reflection to inject the mocked secondary deserializer
        try {
            java.lang.reflect.Field field = AWSKafkaAvroDeserializer.class.getDeclaredField("secondaryDeserializer");
            field.setAccessible(true);
            field.set(deserializer, mockSecondaryDeserializer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mock secondary deserializer", e);
        }
        
        // Configure with secondary deserializer (will use our mock)
        Map<String, Object> configsWithSecondary = new HashMap<>(configs);
        configsWithSecondary.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, "mock.class");
        deserializer.configure(configsWithSecondary, false);
        
        // Test: Confluent data should route to secondary deserializer
        Object result = deserializer.deserialize("test-topic", confluentData);
        
        // Verify results
        assertEquals("secondary-result", result);
        
        // Verify secondary deserializer was called
        Mockito.verify(mockSecondaryDeserializer, Mockito.times(1)).deserialize("test-topic", confluentData);
        
        // Verify primary deserializer was NOT called
        Mockito.verify(mockFacade, Mockito.never()).deserialize(Mockito.any(AWSDeserializerInput.class));
    }

    /**
     * Test that Glue-formatted data (starting with HEADER_VERSION_BYTE) goes to primary deserializer.
     */
    @Test
    public void testPrimaryDeserializer_GlueData_ShouldUsePrimary() {
        // Glue-formatted data: header(1) + compression(1) + UUID(16) + data = 18+ bytes minimum
        byte[] glueData = new byte[20];
        glueData[0] = AWSSchemaRegistryConstants.HEADER_VERSION_BYTE; // 0x03
        glueData[1] = AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE; // compression byte
        // Fill remaining bytes (UUID would be bytes 2-17, data starts at 18)
        for (int i = 2; i < 20; i++) {
            glueData[i] = (byte) i;
        }
        
        // Create deserializer with mocked components
        AWSKafkaAvroDeserializer deserializer = new AWSKafkaAvroDeserializer(mockCredProvider, null);
        
        // Mock the secondary deserializer (should NOT be called)
        SecondaryDeserializer mockSecondaryDeserializer = mock(SecondaryDeserializer.class);
        when(mockSecondaryDeserializer.validate(Mockito.any())).thenReturn(true);
        
        // Use reflection to inject the mocked secondary deserializer
        try {
            java.lang.reflect.Field field = AWSKafkaAvroDeserializer.class.getDeclaredField("secondaryDeserializer");
            field.setAccessible(true);
            field.set(deserializer, mockSecondaryDeserializer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mock secondary deserializer", e);
        }
        
        // Configure with secondary deserializer (will use our mock)
        Map<String, Object> configsWithSecondary = new HashMap<>(configs);
        configsWithSecondary.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, "mock.class");
        deserializer.configure(configsWithSecondary, false);
        
        // Mock the primary deserializer facade AFTER configuration
        GlueSchemaRegistryDeserializationFacade mockFacade = mock(GlueSchemaRegistryDeserializationFacade.class);
        when(mockFacade.deserialize(Mockito.any(AWSDeserializerInput.class))).thenReturn("primary-result");
        deserializer.setGlueSchemaRegistryDeserializationFacade(mockFacade);
        
        // Test: Glue data should route to primary deserializer
        Object result = deserializer.deserialize("test-topic", glueData);
        
        // Verify results
        assertEquals("primary-result", result);
        
        // Verify primary deserializer was called
        Mockito.verify(mockFacade, Mockito.times(1)).deserialize(Mockito.any(AWSDeserializerInput.class));
        
        // Verify secondary deserializer was NOT called
        Mockito.verify(mockSecondaryDeserializer, Mockito.never()).deserialize(Mockito.anyString(), Mockito.any(byte[].class));
    }
}
