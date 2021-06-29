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

package com.amazonaws.services.schemaregistry.kafkastreams;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.kafkastreams.utils.RecordGenerator;
import com.amazonaws.services.schemaregistry.kafkastreams.utils.avro.User;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing AWSKafkaAvroSerDe class.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AWSKafkaAvroSerDeTest {
    @Mock
    private AWSSchemaRegistryClient mockClient;
    @Mock
    private AwsCredentialsProvider mockCredProvider;

    private AWSKafkaAvroSerDe awsKafkaAvroSerDe;
    private Map<String, Object> configs;
    private static final String testTopic = "test-topic";
    private static final UUID schemaVersionIdForTesting = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final byte[] genericBytes = new byte[] {3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
            93, -23, -17, 12, 99, 10, 115, 97, 110, 115, 97, 0, -58, 1, 0, 6, 114, 101, 100};
    private static final byte[] specificBytes = new byte[] {3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
            93, -23, -17, 12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116};

    @BeforeEach
    public void setup() {
        configs = getProperties();
    }

    /**
     * Test for generic record.
     */
    @Test
    public void testSerDe_GenericRecord_DeserializedEqualsSerialized() {
        GenericRecord expected = RecordGenerator.createGenericAvroRecord();
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(expected);

        AWSKafkaAvroSerializer awsKafkaAvroSerializer = createSerializer(schemaDefinition, schemaVersionIdForTesting);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = createDeserializer(expected, genericBytes);
        awsKafkaAvroSerDe = new AWSKafkaAvroSerDe(awsKafkaAvroSerializer, awsKafkaAvroDeserializer);

        GenericRecord genericRecord = (GenericRecord) awsKafkaAvroSerDe.deserializer().deserialize(
                testTopic, awsKafkaAvroSerDe.serializer().serialize(testTopic, expected));

        assertEquals(expected, genericRecord);
    }

    /**
     * Test for specific record.
     */
    @Test
    public void testSerDe_SpecificRecord_DeserializedEqualsSerialized() {
        User expected = RecordGenerator.createSpecificAvroRecord();
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(expected);

        AWSKafkaAvroSerializer awsKafkaAvroSerializer = createSerializer(schemaDefinition, schemaVersionIdForTesting);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = createDeserializer(expected, specificBytes);
        awsKafkaAvroSerDe = new AWSKafkaAvroSerDe(awsKafkaAvroSerializer, awsKafkaAvroDeserializer);

        User user = (User) awsKafkaAvroSerDe.deserializer().deserialize(
                testTopic, awsKafkaAvroSerDe.serializer().serialize(testTopic, expected));

        assertEquals(expected, user);
    }

    /***
     * Tests the constructor with no parameters
     */
    @Test
    public void testConstructor_noParameters_succeeds() {
        awsKafkaAvroSerDe = new AWSKafkaAvroSerDe();
        assertNotNull(awsKafkaAvroSerDe);
        assertNotNull(awsKafkaAvroSerDe.serializer());
        assertNotNull(awsKafkaAvroSerDe.deserializer());
    }

    /**
     * Tests invoking close method.
     */
    @Test
    public void testClose_succeeds() {
        awsKafkaAvroSerDe = createTestAWSKafkaAvroSerDe();
        assertDoesNotThrow(() -> awsKafkaAvroSerDe.close());
    }

    /***
     * Test the invocation of configure method
     */
    @Test public void testConfigure_succeeds() {
        awsKafkaAvroSerDe = createTestAWSKafkaAvroSerDe();
        assertDoesNotThrow(() -> awsKafkaAvroSerDe.configure(configs, false));
    }

    /**
     * To create a AWSKafkaAvroSerializer instance with mocked parameters.
     *
     * @return a mocked AWSKafkaAvroSerializer instance
     */
    private AWSKafkaAvroSerializer createSerializer(String schemaDefinition,
                                                    UUID schemaVersionId) {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(mockCredProvider)
                        .schemaRegistryClient(mockClient)
                        .build();

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User-Topic"),
                                                     eq(DataFormat.AVRO.name()), anyMap())).thenReturn(schemaVersionId);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = new AWSKafkaAvroSerializer(mockCredProvider, null);
        awsKafkaAvroSerializer.configure(configs, true);

        awsKafkaAvroSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);

        return awsKafkaAvroSerializer;
    }

    /**
     * To create a AWSKafkaAvroDeserializer instance with mocked parameters.
     *
     * @return a mocked AWSKafkaAvroDeserializer instance
     */
    private AWSKafkaAvroDeserializer createDeserializer(Object record,
                                                        byte[] bytes) {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                mock(GlueSchemaRegistryDeserializationFacade.class);
        AWSDeserializerInput awsDeserializerInput = AWSDeserializerInput.builder()
                .buffer(ByteBuffer.wrap(bytes))
                .transportName(testTopic)
                .build();

        when(glueSchemaRegistryDeserializationFacade.deserialize(awsDeserializerInput)).thenReturn(record);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(mockCredProvider, null);
        awsKafkaAvroDeserializer.configure(configs, true);

        awsKafkaAvroDeserializer.setGlueSchemaRegistryDeserializationFacade(glueSchemaRegistryDeserializationFacade);

        return awsKafkaAvroDeserializer;
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");

        return props;
    }

    private AWSKafkaAvroSerDe createTestAWSKafkaAvroSerDe() {
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(RecordGenerator.createGenericAvroRecord());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = createSerializer(schemaDefinition, schemaVersionIdForTesting);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = createDeserializer(RecordGenerator.createGenericAvroRecord(), genericBytes);
        return new AWSKafkaAvroSerDe(awsKafkaAvroSerializer, awsKafkaAvroDeserializer);
    }
}