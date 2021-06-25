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
package com.amazonaws.services.schemaregistry.deserializers;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing Kafka specific de-serializer.
 */
@ExtendWith(MockitoExtension.class)
public class GlueSchemaRegistryKafkaDeserializerTest {
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
     * Test GlueSchemaRegistryKafkaDeserializer instantiation.
     */
    @Test
    public void test_Create() {
        // Test create with empty constructor
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer1 = new GlueSchemaRegistryKafkaDeserializer();
        assertNotNull(glueSchemaRegistryKafkaDeserializer1.getCredentialProvider());

        // Test create with AWSCredentialsProvider constructor
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer2 = new GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider,
                configs);
        assertNotNull(glueSchemaRegistryKafkaDeserializer2.getCredentialProvider());
    }

    @Test
    public void test_Create_With_Aws_Deserializer() {
        // Test create with empty constructor
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, null);
        glueSchemaRegistryKafkaDeserializer.setGlueSchemaRegistryDeserializationFacade(
                GlueSchemaRegistryDeserializationFacade.builder().credentialProvider(this.mockCredProvider).configs(configs).build());

        assertNotNull(glueSchemaRegistryKafkaDeserializer.getCredentialProvider());
        assertNotNull(glueSchemaRegistryKafkaDeserializer.getGlueSchemaRegistryDeserializationFacade());
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer configure method for empty configuration.
     */
    @Test
    public void test_Configure_Empty_Config() {
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer();
        assertNotNull(glueSchemaRegistryKafkaDeserializer.getCredentialProvider());

        Map<String, Object> configs = new HashMap<>();
        assertThrows(IllegalArgumentException.class, () -> glueSchemaRegistryKafkaDeserializer.configure(configs, false));
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer deserialize method by mocking the dependency.
     */
    @Test
    public void test_Deserialize_Null_Input() {
        // Mock the dependency
        GlueSchemaRegistryDeserializationFacade
                glueSchemaRegistryDeserializationFacade = mock(GlueSchemaRegistryDeserializationFacade.class);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, null);
        glueSchemaRegistryKafkaDeserializer.setGlueSchemaRegistryDeserializationFacade(glueSchemaRegistryDeserializationFacade);

        Object result = glueSchemaRegistryKafkaDeserializer.deserialize("TestTopic", null);
        assertNull(result);
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer deserialize method by mocking the dependency.
     */
    @Test
    public void test_Deserialize() {
        Object expectedObject = new Object();
        // Mock the dependency
        GlueSchemaRegistryDeserializationFacade
                glueSchemaRegistryDeserializationFacade = mock(GlueSchemaRegistryDeserializationFacade.class);
        when(glueSchemaRegistryDeserializationFacade.deserialize(Mockito.any(AWSDeserializerInput.class))).thenReturn(expectedObject);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, null);
        glueSchemaRegistryKafkaDeserializer.setGlueSchemaRegistryDeserializationFacade(glueSchemaRegistryDeserializationFacade);

        Object deserializedObject = glueSchemaRegistryKafkaDeserializer.deserialize("TestTopic",
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
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, null);
        glueSchemaRegistryKafkaDeserializer.setGlueSchemaRegistryDeserializationFacade(glueSchemaRegistryDeserializationFacade);

        Mockito.verify(glueSchemaRegistryDeserializationFacade, Mockito.atMost(1)).close();
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer configure method for null pointer exception by passing null config
     */
    @Test
    public void testConfigure_nullConfig_throwsException() {
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer();
        assertNotNull(glueSchemaRegistryKafkaDeserializer.getCredentialProvider());

        assertThrows((IllegalArgumentException.class), () -> glueSchemaRegistryKafkaDeserializer.configure(null, false));
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer configure method for positive scenario by passing valid config.
     */
    @Test
    public void testConfigure_validConfig_throwsException() {
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer();
        assertNotNull(glueSchemaRegistryKafkaDeserializer.getCredentialProvider());

        assertDoesNotThrow(() -> glueSchemaRegistryKafkaDeserializer.configure(configs, false));
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer constructor for null pointer exception by passing null config.
     */
    @Test
    public void testConstructor_nullConfig_throwsException() {
        assertThrows((IllegalArgumentException.class), () -> new GlueSchemaRegistryKafkaDeserializer(null));
    }

    /**
     * Tests invoking close method.
     */
    @Test
    public void testClose_succeeds() {
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer();
        glueSchemaRegistryKafkaDeserializer.configure(configs, false);

        assertDoesNotThrow(() -> glueSchemaRegistryKafkaDeserializer.close());
    }
}
