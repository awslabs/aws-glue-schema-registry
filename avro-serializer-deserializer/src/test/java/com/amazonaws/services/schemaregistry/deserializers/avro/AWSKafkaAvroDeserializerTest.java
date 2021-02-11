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

import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER;
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

import com.amazonaws.services.schemaregistry.deserializers.AWSDeserializer;

import org.apache.kafka.common.serialization.Deserializer;
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
        awsKafkaAvroDeserializer.setAwsDeserializer(
                AWSDeserializer.builder().credentialProvider(this.mockCredProvider).configs(configs).build());

        assertNotNull(awsKafkaAvroDeserializer.getCredentialProvider());
        assertNotNull(awsKafkaAvroDeserializer.getAwsDeserializer());
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
        AWSDeserializer awsDeserializer = mock(AWSDeserializer.class);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(this.mockCredProvider, null);
        awsKafkaAvroDeserializer.setAwsDeserializer(awsDeserializer);

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
        AWSDeserializer awsDeserializer = mock(AWSDeserializer.class);
        when(awsDeserializer.deserialize(Mockito.any(AWSDeserializerInput.class))).thenReturn(expectedObject);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(this.mockCredProvider, null);
        awsKafkaAvroDeserializer.setAwsDeserializer(awsDeserializer);

        Object deserializedObject = awsKafkaAvroDeserializer.deserialize("TestTopic",
                new byte[] { AWSSchemaRegistryConstants.HEADER_VERSION_BYTE });
        assertEquals(expectedObject, deserializedObject);
    }

    /**
     * Tests invoking shutdown invokes the internal AWSDeserializer.close method.
     */
    @Test
    public void testClose_callInternalAWSDeserializer_succeeds() {
        AWSDeserializer awsDeserializer = mock(AWSDeserializer.class);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(this.mockCredProvider, null);
        awsKafkaAvroDeserializer.setAwsDeserializer(awsDeserializer);

        Mockito.verify(awsDeserializer, Mockito.atMost(1)).close();
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
     * Test asserting that the secondaryDeserializer in {@link AWSKafkaAvroDeserializer}
     * can be configured independently for each instance of {@link AWSKafkaAvroDeserializer}.
     */
    @Test
    public void test_deserialize_assureSecondaryDeserializersAreIndependent(){
        // given - 2 AWSKafkaAvroDeserializer deserializer instances both with secondary deserializers,
        // each having a unique configuration
        Map<String, Object> config_1 = new HashMap<>(configs);
        config_1.put(SECONDARY_DESERIALIZER, TestingDeserializerImpl.class.getName());
        config_1.put(TestingDeserializerImpl.CUSTOM_CONFIG_KEY, "firstValue");
        AWSKafkaAvroDeserializer deserializer_1 = new AWSKafkaAvroDeserializer(config_1);

        Map<String, Object> config_2 = new HashMap<>(configs);
        config_2.put(SECONDARY_DESERIALIZER, TestingDeserializerImpl.class.getName());
        config_2.put(TestingDeserializerImpl.CUSTOM_CONFIG_KEY, "secondValue");
        AWSKafkaAvroDeserializer deserializer_2 = new AWSKafkaAvroDeserializer(config_2);

        byte[] testValue = new byte[1];
        testValue[0] = (byte) 0; // some value other than (byte) 3 which would be the HEADER_VERSION_BYTE for the AWSKafkaAvroDeserializer itself to be called

        // when - calling both deserializers
        Object deserializedValue_1 = deserializer_1.deserialize(null, testValue);
        Object deserializedValue_2 = deserializer_2.deserialize(null, testValue);

        // then - each deserializer delegated the deserialization call to it's own custom configured secondary deserializer
        // this proves we are able to configure the secondary deserializers independently
        assertEquals("firstValue", deserializedValue_1);
        assertEquals("secondValue", deserializedValue_2);
    }

    public static class TestingDeserializerImpl implements Deserializer<Object> {
        public static final String CUSTOM_CONFIG_KEY = "secondary.custom.config";
        private String value;

        @Override
        public Object deserialize(final String s, final byte[] bytes) {
            return value;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            value = (String) configs.get(CUSTOM_CONFIG_KEY);
        }
    }
}
