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
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.kafkastreams.utils.RecordGenerator;
import com.amazonaws.services.schemaregistry.kafkastreams.utils.avro.User;
import com.amazonaws.services.schemaregistry.kafkastreams.utils.json.Car;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.serializers.json.JsonSerializer;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing GlueSchemaRegistryKafkaStreamsSerde class.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class GlueSchemaRegistryKafkaStreamsSerdeTest {
    private static final String testTopic = "test-topic";
    private static final String schemaName = "User-Topic";
    private static final UUID schemaVersionIdForTesting = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final byte[] avroGenericBytes =
            new byte[]{3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 10, 115, 97
                    , 110, 115, 97, 0, -58, 1, 0, 6, 114, 101, 100};
    private static final byte[] avroSpecificBytes =
            new byte[]{3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 8, 116, 101
                    , 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116};
    private static final byte[] jsonGenericBytes =
            new byte[]{3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 123, 34,
                    102, 105, 114, 115, 116, 78, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 34, 44, 34, 108, 97,
                    115, 116, 78, 97, 109, 101, 34, 58, 34, 68, 111, 101, 34, 44, 34, 97, 103, 101, 34, 58, 50, 49,
                    125};
    private static final byte[] jsonSpecificBytes =
            new byte[]{3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 123, 34,
                    109, 97, 107, 101, 34, 58, 34, 72, 111, 110, 100, 97, 34, 44, 34, 109, 111, 100, 101, 108, 34, 58
                    , 34, 99, 114, 118, 34, 44, 34, 117, 115, 101, 100, 34, 58, 116, 114, 117, 101, 44, 34, 109, 105,
                    108, 101, 115, 34, 58, 49, 48, 48, 48, 48, 44, 34, 121, 101, 97, 114, 34, 58, 50, 48, 49, 54, 44,
                    34, 112, 117, 114, 99, 104, 97, 115, 101, 68, 97, 116, 101, 34, 58, 57, 52, 54, 54, 56, 52, 56,
                    48, 48, 48, 48, 48, 44, 34, 108, 105, 115, 116, 101, 100, 68, 97, 116, 101, 34, 58, 49, 51, 57,
                    50, 49, 48, 53, 54, 48, 48, 48, 48, 48, 44, 34, 111, 119, 110, 101, 114, 115, 34, 58, 91, 34, 74,
                    111, 104, 110, 34, 44, 34, 74, 97, 110, 101, 34, 44, 34, 72, 117, 34, 93, 44, 34, 115, 101, 114,
                    118, 105, 99, 101, 67, 104, 101, 99, 107, 115, 34, 58, 91, 53, 48, 48, 48, 46, 48, 44, 49, 48, 55
                    , 56, 48, 46, 51, 93, 125};

    @Mock
    private AWSSchemaRegistryClient mockClient;
    @Mock
    private AwsCredentialsProvider mockCredProvider;
    private GlueSchemaRegistryKafkaStreamsSerde glueSchemaRegistryKafkaStreamsSerde;
    private Map<String, Object> configs;

    /**
     * Test for AVRO generic record.
     */
    @Test
    public void testSerDe_AvroGenericRecord_DeserializedEqualsSerialized() {
        configs = getProperties(DataFormat.AVRO, AvroRecordType.GENERIC_RECORD);

        GenericRecord expected = RecordGenerator.createGenericAvroRecord();
        String schemaDefinition = AVROUtils.getInstance()
                .getSchemaDefinition(expected);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                createSerializer(schemaDefinition, schemaVersionIdForTesting, DataFormat.AVRO);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                createDeserializer(expected, avroGenericBytes);
        glueSchemaRegistryKafkaStreamsSerde = new GlueSchemaRegistryKafkaStreamsSerde(glueSchemaRegistryKafkaSerializer,
                                                                                      glueSchemaRegistryKafkaDeserializer);

        GenericRecord genericRecord = (GenericRecord) glueSchemaRegistryKafkaStreamsSerde.deserializer()
                .deserialize(testTopic, glueSchemaRegistryKafkaStreamsSerde.serializer()
                        .serialize(testTopic, expected));

        assertEquals(expected, genericRecord);
    }

    /**
     * Test for AVRO specific record.
     */
    @Test
    public void testSerDe_AvroSpecificRecord_DeserializedEqualsSerialized() {
        configs = getProperties(DataFormat.AVRO, AvroRecordType.SPECIFIC_RECORD);

        User expected = RecordGenerator.createSpecificAvroRecord();
        String schemaDefinition = AVROUtils.getInstance()
                .getSchemaDefinition(expected);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                createSerializer(schemaDefinition, schemaVersionIdForTesting, DataFormat.AVRO);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                createDeserializer(expected, avroSpecificBytes);
        glueSchemaRegistryKafkaStreamsSerde = new GlueSchemaRegistryKafkaStreamsSerde(glueSchemaRegistryKafkaSerializer,
                                                                                      glueSchemaRegistryKafkaDeserializer);

        User user = (User) glueSchemaRegistryKafkaStreamsSerde.deserializer()
                .deserialize(testTopic, glueSchemaRegistryKafkaStreamsSerde.serializer()
                        .serialize(testTopic, expected));

        assertEquals(expected, user);
    }

    /**
     * Test for JSON generic record.
     */
    @Test
    public void testSerDe_JsonGenericRecord_DeserializedEqualsSerialized() {
        configs = getProperties(DataFormat.JSON, AvroRecordType.GENERIC_RECORD);

        JsonDataWithSchema expected = RecordGenerator.createGenericJsonRecord();

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                createSerializer(expected.getSchema(), schemaVersionIdForTesting, DataFormat.JSON);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                createDeserializer(expected, jsonGenericBytes);
        glueSchemaRegistryKafkaStreamsSerde = new GlueSchemaRegistryKafkaStreamsSerde(glueSchemaRegistryKafkaSerializer,
                                                                                      glueSchemaRegistryKafkaDeserializer);

        JsonDataWithSchema deserialized = (JsonDataWithSchema) glueSchemaRegistryKafkaStreamsSerde.deserializer()
                .deserialize(testTopic, glueSchemaRegistryKafkaStreamsSerde.serializer()
                        .serialize(testTopic, expected));

        assertEquals(expected, deserialized);
    }

    /**
     * Test for JSON specific record.
     */
    @Test
    public void testSerDe_JsonSpecificRecord_DeserializedEqualsSerialized() {
        configs = getProperties(DataFormat.JSON, AvroRecordType.GENERIC_RECORD);

        Car expected = RecordGenerator.createSpecificJsonRecord();
        JsonSerializer jsonSerializer =
                new JsonSerializer(new GlueSchemaRegistryConfiguration(new HashMap<String, String>() {{
                    put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
                }}));
        String schemaDefinition = jsonSerializer.getSchemaDefinition(expected);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                createSerializer(schemaDefinition, schemaVersionIdForTesting, DataFormat.JSON);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                createDeserializer(expected, jsonSpecificBytes);
        glueSchemaRegistryKafkaStreamsSerde = new GlueSchemaRegistryKafkaStreamsSerde(glueSchemaRegistryKafkaSerializer,
                                                                                      glueSchemaRegistryKafkaDeserializer);

        Car car = (Car) glueSchemaRegistryKafkaStreamsSerde.deserializer()
                .deserialize(testTopic, glueSchemaRegistryKafkaStreamsSerde.serializer()
                        .serialize(testTopic, expected));

        assertEquals(expected, car);
    }

    /**
     * Test for null record.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testSerde_NullRecord_DeserializedEqualsSerializedAsNull(DataFormat dataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                mock(GlueSchemaRegistryKafkaSerializer.class);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                mock(GlueSchemaRegistryKafkaDeserializer.class);
        glueSchemaRegistryKafkaStreamsSerde = new GlueSchemaRegistryKafkaStreamsSerde(glueSchemaRegistryKafkaSerializer,
                                                                                      glueSchemaRegistryKafkaDeserializer);

        byte[] serialized = glueSchemaRegistryKafkaStreamsSerde.serializer()
                .serialize(testTopic, null);
        Object deserialized = glueSchemaRegistryKafkaStreamsSerde.deserializer()
                .deserialize(testTopic, serialized);
        assertNull(serialized);
        assertNull(deserialized);
    }

    /**
     * Test for empty record.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testSerde_EmptyRecord_DeserializedEqualsSerializedAsNull(DataFormat dataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                mock(GlueSchemaRegistryKafkaSerializer.class);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                mock(GlueSchemaRegistryKafkaDeserializer.class);
        glueSchemaRegistryKafkaStreamsSerde = new GlueSchemaRegistryKafkaStreamsSerde(glueSchemaRegistryKafkaSerializer,
                                                                                      glueSchemaRegistryKafkaDeserializer);

        byte[] serialized = glueSchemaRegistryKafkaStreamsSerde.serializer()
                .serialize(testTopic, "");
        Object deserialized = glueSchemaRegistryKafkaStreamsSerde.deserializer()
                .deserialize(testTopic, serialized);
        assertNull(serialized);
        assertNull(deserialized);
    }

    /***
     * Tests the constructor with no parameters
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testConstructor_noParameters_succeeds(DataFormat dataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD);
        glueSchemaRegistryKafkaStreamsSerde = new GlueSchemaRegistryKafkaStreamsSerde();
        assertNotNull(glueSchemaRegistryKafkaStreamsSerde);
        assertNotNull(glueSchemaRegistryKafkaStreamsSerde.serializer());
        assertNotNull(glueSchemaRegistryKafkaStreamsSerde.deserializer());
    }

    /**
     * Tests invoking close method.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testClose_succeeds(DataFormat dataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD);
        glueSchemaRegistryKafkaStreamsSerde = createTestGlueSchemaRegistryKafkaStreamsSerde(dataFormat);
        assertDoesNotThrow(() -> glueSchemaRegistryKafkaStreamsSerde.close());
    }

    /***
     * Test the invocation of configure method
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testConfigure_succeeds(DataFormat dataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD);
        glueSchemaRegistryKafkaStreamsSerde = createTestGlueSchemaRegistryKafkaStreamsSerde(dataFormat);
        assertDoesNotThrow(() -> glueSchemaRegistryKafkaStreamsSerde.configure(configs, false));
    }

    /**
     * To create a GlueSchemaRegistryKafkaSerializer instance with mocked parameters.
     *
     * @return a mocked GlueSchemaRegistryKafkaSerializer instance
     */
    private GlueSchemaRegistryKafkaSerializer createSerializer(String schemaDefinition,
                                                               UUID schemaVersionId,
                                                               DataFormat dataFormat) {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(mockCredProvider)
                        .schemaRegistryClient(mockClient)
                        .build();

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq(schemaName), eq(dataFormat.name()),
                                                     anyMap())).thenReturn(schemaVersionId);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                new GlueSchemaRegistryKafkaSerializer(mockCredProvider, null);
        glueSchemaRegistryKafkaSerializer.configure(configs, true);

        glueSchemaRegistryKafkaSerializer.setGlueSchemaRegistrySerializationFacade(
                glueSchemaRegistrySerializationFacade);

        return glueSchemaRegistryKafkaSerializer;
    }

    /**
     * To create a GlueSchemaRegistryKafkaDeserializer instance with mocked parameters.
     *
     * @return a mocked GlueSchemaRegistryKafkaDeserializer instance
     */
    private GlueSchemaRegistryKafkaDeserializer createDeserializer(Object record,
                                                                   byte[] bytes) {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                mock(GlueSchemaRegistryDeserializationFacade.class);
        AWSDeserializerInput awsDeserializerInput = AWSDeserializerInput.builder()
                .buffer(ByteBuffer.wrap(bytes))
                .transportName(testTopic)
                .build();

        when(glueSchemaRegistryDeserializationFacade.deserialize(awsDeserializerInput)).thenReturn(record);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                new GlueSchemaRegistryKafkaDeserializer(mockCredProvider, null);
        glueSchemaRegistryKafkaDeserializer.configure(configs, true);

        glueSchemaRegistryKafkaDeserializer.setGlueSchemaRegistryDeserializationFacade(
                glueSchemaRegistryDeserializationFacade);

        return glueSchemaRegistryKafkaDeserializer;
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getProperties(DataFormat dataFormat,
                                              AvroRecordType recordType) {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, schemaName);
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, recordType.name()); // Only required for AVRO case

        return props;
    }

    private GlueSchemaRegistryKafkaStreamsSerde createTestGlueSchemaRegistryKafkaStreamsSerde(DataFormat dataFormat) {
        Object record;
        String schemaDefinition;
        byte[] expectedBytes;
        switch (dataFormat) {
            case AVRO:
                record = RecordGenerator.createGenericAvroRecord();
                schemaDefinition = AVROUtils.getInstance()
                        .getSchemaDefinition(record);
                expectedBytes = avroGenericBytes;
                break;
            case JSON:
                record = RecordGenerator.createGenericJsonRecord();
                schemaDefinition = ((JsonDataWithSchema) record).getSchema();
                expectedBytes = jsonGenericBytes;
                break;
            default:
                throw new RuntimeException("Data format is not supported");
        }

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                createSerializer(schemaDefinition, schemaVersionIdForTesting, dataFormat);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                createDeserializer(record, expectedBytes);
        return new GlueSchemaRegistryKafkaStreamsSerde(glueSchemaRegistryKafkaSerializer,
                                                       glueSchemaRegistryKafkaDeserializer);
    }
}
