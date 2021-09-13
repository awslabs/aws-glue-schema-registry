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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class SecondaryDeserializerTest {
    private final Map<String, Object> configs = new HashMap<>();
    private SecondaryDeserializer secondaryDeserializer;
    private GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs;
    private static final String VALID_SECONDARY_DESERIALIZER =
            "com.amazonaws.services.schemaregistry.deserializers.external.ThirdPartyDeserializer";
    private static final String NON_KAFKA_SECONDARY_DESERIALIZER =
            "com.amazonaws.services.schemaregistry.deserializers.external.NotKafkaDeserializer";
    private static final String EMPTY_SECONDARY_DESERIALIZER = "";
    private static final String NON_KAFKA_SECONDARY_DESERIALIZER_EXCEPTION_MSG =
            "The secondary deserializer is not from Kafka";
    private static final String EMPTY_SECONDARY_DESERIALIZER_EXCEPTION_MSG = "Can't find the class or instantiate it.";
    private static final String NULL_SECONDARY_DESERIALIZER_EXCEPTION_MSG = "Invalid secondary de-serializer configuration";
    private static final String TEST_TOPIC = "TestTopic";

    @Mock
    private AwsCredentialsProvider mockCredProvider;
    @Mock
    private GlueSchemaRegistryDeserializationFacade mockGlueSchemaRegistryDeserializationFacade;

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    public void setup() {
        secondaryDeserializer = SecondaryDeserializer.newInstance();

        this.configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        this.configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        this.schemaRegistrySerDeConfigs = new GlueSchemaRegistryConfiguration(this.configs);
    }

    @Test
    public void testDeserialize_nullObjField_throwsException() throws IllegalAccessException, NoSuchFieldException {
        Field objField = SecondaryDeserializer.class.getDeclaredField("obj");
        objField.setAccessible(true);
        objField.set(secondaryDeserializer, null);

        assertThrows(AWSSchemaRegistryException.class , () -> secondaryDeserializer.deserialize(null, null));
    }

    @Test
    public void testClose_nullObjField_throwsException() throws IllegalAccessException, NoSuchFieldException{
        Field objField = SecondaryDeserializer.class.getDeclaredField("obj");
        objField.setAccessible(true);
        objField.set(secondaryDeserializer, null);

        assertThrows(AWSSchemaRegistryException.class , () -> secondaryDeserializer.close());
    }

    /**
     * Tests the SecondaryDeserializer by importing a valid third party kafka deserializer
     */
    @Test
    public void testSecondaryDeserializer_validDeserializer_deserializesSuccessfully() {
        byte[] serializedBytes = { 0000 };

        Map<String, Object> configs = getConfigsWithSecondaryDeserializer(VALID_SECONDARY_DESERIALIZER);
        Object deserializedObject = deserialize(configs, serializedBytes);

        assertNotNull(deserializedObject);
        assertTrue(deserializedObject instanceof Object);
    }


    /**
     * Tests the SecondaryDeserializer by importing a String kafka deserializer
     */
    @Test
    public void testSecondaryDeserializer_withStringDeserializer_deserializesSuccessfully() {
        StringSerializer stringSerializer = new StringSerializer();
        final String objectToSerialize = "TestJsonRecord";
        byte[] serializedBytes = stringSerializer.serialize(TEST_TOPIC, objectToSerialize);

        Map<String, Object> deserializerConfigs = getConfigsWithSecondaryDeserializer(StringDeserializer.class.getName());
        Object deserializedObject = deserialize(deserializerConfigs, serializedBytes);

        assertNotNull(deserializedObject);
        assertTrue(deserializedObject instanceof String);
        assertEquals(objectToSerialize, deserializedObject);
    }

    /**
     * Tests the SecondaryDeserializer by importing a Integer kafka deserializer
     */
    @Test
    public void testSecondaryDeserializer_withIntegerDeserializer_deserializesSuccessfully() {
        IntegerSerializer integerSerializer = new IntegerSerializer();
        final Integer objectToSerialize = 1;
        byte[] serializedBytes = integerSerializer.serialize(TEST_TOPIC, objectToSerialize);

        Map<String, Object> deserializerConfigs = getConfigsWithSecondaryDeserializer(IntegerDeserializer.class.getName());
        Object deserializedObject = deserialize(deserializerConfigs, serializedBytes);

        assertNotNull(deserializedObject);
        assertTrue(deserializedObject instanceof Integer);
        assertEquals(objectToSerialize, deserializedObject);
    }

    /**
     * Tests the SecondaryDeserializer by importing deserializer not from Kafka - negative case.
     */
    @Test
    public void testSecondaryDeserializer_invalidDeserializer_throwsException() {
        Map<String, Object> configs = getConfigsWithSecondaryDeserializer(NON_KAFKA_SECONDARY_DESERIALIZER);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer();

        Exception exception = assertThrows(AWSSchemaRegistryException.class,
                () -> glueSchemaRegistryKafkaDeserializer.configure(configs, true));

        assertEquals(NON_KAFKA_SECONDARY_DESERIALIZER_EXCEPTION_MSG, exception.getMessage());
    }

    /**
     * Tests the SecondaryDeserializer by importing empty secondary deserializer - negative case.
     */
    @Test
    public void testSecondaryDeserializer_emptyDeserializer_throwsException() {
        Map<String, Object> configs = getConfigsWithSecondaryDeserializer(EMPTY_SECONDARY_DESERIALIZER);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer();

        Exception exception = assertThrows(AWSSchemaRegistryException.class,
                () -> glueSchemaRegistryKafkaDeserializer.configure(configs, true));

        assertEquals(EMPTY_SECONDARY_DESERIALIZER_EXCEPTION_MSG, exception.getMessage());
    }

    /**
     * Tests the SecondaryDeserializer by importing null secondary deserializer - negative case.
     */
    @Test
    public void testSecondaryDeserializer_nullDeserializer_throwsException() {
        Map<String, Object> configs = getConfigsWithSecondaryDeserializer(null);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer();

        Exception exception = assertThrows(AWSSchemaRegistryException.class,
                () -> glueSchemaRegistryKafkaDeserializer.configure(configs, true));

        assertEquals(NULL_SECONDARY_DESERIALIZER_EXCEPTION_MSG, exception.getMessage());
    }

    private Map<String, Object> getConfigsWithSecondaryDeserializer(String className) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, className);
        return configs;
    }

    private Object deserialize(Map<String, Object> deserializerConfigs, byte[] serializedBytes) {
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider,
                deserializerConfigs);
        glueSchemaRegistryKafkaDeserializer.setGlueSchemaRegistryDeserializationFacade(mockGlueSchemaRegistryDeserializationFacade);
        return glueSchemaRegistryKafkaDeserializer.deserialize(TEST_TOPIC, serializedBytes);
    }

}
