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

package com.amazonaws.services.schemaregistry.serializers.avro;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory;
import com.amazonaws.services.schemaregistry.serializers.json.Car;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class GlueSchemaRegistrySerializationFacadeTest extends GlueSchemaRegistryValidationUtil {
    public static final String AVRO_USER_SCHEMA_FILE = "src/test/resources/avro/user.avsc";
    public static final String AVRO_USER_ARRAY_STRING_SCHEMA_FILE = "src/test/resources/avro/user_array_String.avsc";
    private static final UUID SCHEMA_VERSION_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final String TRANSPORT_NAME = "default-stream";
    private static final String TEST_SCHEMA = "test-schema";
    private static final String USER_SCHEMA = "User";
    private static final String TEST_TOPIC = "test-topic";
    private static final String USER_TOPIC = "User-Topic";
    private static final GenericRecord genericAvroRecord = RecordGenerator.createGenericAvroRecord();
    private static final GenericData.EnumSymbol genericUserEnumAvroRecord =
            RecordGenerator.createGenericUserEnumAvroRecord();
    private static final GenericData.Array<Integer> genericIntArrayAvroRecord =
            RecordGenerator.createGenericIntArrayAvroRecord();
    private static final GenericData.Array<String> genericStringArrayAvroRecord =
            RecordGenerator.createGenericStringArrayAvroRecord();
    private static final GenericData.EnumSymbol genericRecordInvalidEnumData =
            RecordGenerator.createGenericUserInvalidEnumAvroRecord();
    private static final GenericData.Array<Object> genericRecordInvalidArrayData =
            RecordGenerator.createGenericUserInvalidArrayAvroRecord();
    private static final GenericData.Record genericUserMapAvroRecord = RecordGenerator.createGenericUserMapAvroRecord();
    private static final GenericData.Record genericInvalidMapAvroRecord =
            RecordGenerator.createGenericInvalidMapAvroRecord();
    private static final GenericData.Record genericUserUnionAvroRecord =
            RecordGenerator.createGenericUserUnionAvroRecord();
    private static final GenericData.Record genericUserUnionNullAvroRecord =
            RecordGenerator.createGenericUnionWithNullValueAvroRecord();
    private static final GenericData.Record genericInvalidUnionAvroRecord =
            RecordGenerator.createGenericInvalidUnionAvroRecord();
    private static final GenericData.Fixed genericFixedAvroRecord = RecordGenerator.createGenericFixedAvroRecord();
    private static final GenericData.Fixed genericInvalidFixedAvroRecord =
            RecordGenerator.createGenericInvalidFixedAvroRecord();
    private static final GenericData.Record genericMultipleTypesAvroRecord =
            RecordGenerator.createGenericMultipleTypesAvroRecord();
    private static final Car specificJsonCarRecord = RecordGenerator.createSpecificJsonRecord();
    private static final Car invalidSpecificJsonCarRecord = RecordGenerator.createInvalidSpecificJsonRecord();
    private static final Object specificNullCarRecord = RecordGenerator.createNullSpecificJsonRecord();
    private static final User userDefinedPojoAvro = RecordGenerator.createSpecificAvroRecord();
    private static GlueSchemaRegistrySerializerFactory glueSchemaRegistrySerializerFactory =
            new GlueSchemaRegistrySerializerFactory();
    private final Map<String, Object> configs = new HashMap<>();
    private Schema schema = null;
    private Customer customer;
    @Mock
    private AwsCredentialsProvider cred;

    @Mock
    private SchemaByDefinitionFetcher mockSchemaByDefinitionFetcher;

    private static List<Arguments> testDataAndSchemaProvider() {
        List<Object> avroRecords =
                Arrays.asList(genericAvroRecord, genericUserEnumAvroRecord, genericIntArrayAvroRecord,
                              genericStringArrayAvroRecord, genericUserMapAvroRecord, genericUserUnionAvroRecord,
                              genericUserUnionNullAvroRecord, genericFixedAvroRecord, genericMultipleTypesAvroRecord,
                              userDefinedPojoAvro);

        List<Object> jsonRecords = Arrays.stream(RecordGenerator.TestJsonRecord.values())
                .filter(RecordGenerator.TestJsonRecord::isValid)
                .map(RecordGenerator::createGenericJsonRecord)
                .collect(Collectors.toList());

        jsonRecords.add(specificJsonCarRecord);

        AWSSchemaRegistryConstants.COMPRESSION[] compressions = AWSSchemaRegistryConstants.COMPRESSION.values();

        List<Arguments> args = new ArrayList<>();

        for (AWSSchemaRegistryConstants.COMPRESSION compression : compressions) {
            args.addAll(avroRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.AVRO, r, compression))
                                .collect(Collectors.toList()));
            args.addAll(jsonRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.JSON, r, compression))
                                .collect(Collectors.toList()));
        }

        return args;
    }

    private static List<Arguments> testInvalidDataAndSchemaProvider() {
        List<Object> avroInvalidRecords =
                Arrays.asList(genericRecordInvalidEnumData, genericRecordInvalidArrayData, genericInvalidMapAvroRecord,
                              genericInvalidUnionAvroRecord, genericInvalidFixedAvroRecord);

        List<Object> jsonInvalidRecords = Arrays.stream(RecordGenerator.TestJsonRecord.values())
                .filter(r -> !r.isValid())
                .map(RecordGenerator::createGenericJsonRecord)
                .collect(Collectors.toList());

        // Invalid JSON -> An Avro record sent instead of JSON
        jsonInvalidRecords.add(genericRecordInvalidEnumData);
        jsonInvalidRecords.add(specificNullCarRecord);
        // Invalid specific record that does not conform to schema defined by POJO
        jsonInvalidRecords.add(invalidSpecificJsonCarRecord);

        AWSSchemaRegistryConstants.COMPRESSION[] compressions = AWSSchemaRegistryConstants.COMPRESSION.values();

        List<Arguments> args = new ArrayList<>();

        for (AWSSchemaRegistryConstants.COMPRESSION compression : compressions) {
            args.addAll(avroInvalidRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.AVRO, r, compression))
                                .collect(Collectors.toList()));

            args.addAll(jsonInvalidRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.JSON, r, compression))
                                .collect(Collectors.toList()));
        }

        return args;
    }

    @BeforeEach
    public void setup() {
        mockSchemaByDefinitionFetcher = mock(SchemaByDefinitionFetcher.class);
        cred = mock(AwsCredentialsProvider.class);
        MockitoAnnotations.initMocks(this);
        customer = new Customer();
        customer.setName("test");
        Map<String, String> metadata = getMetadata();
        Map<String, String> testTags = new HashMap<>();
        testTags.put("testKey", "testValue");

        Schema.Parser parser = new Schema.Parser();
        try {
            schema = parser.parse(new File(AVRO_USER_SCHEMA_FILE));
        } catch (IOException e) {
            fail("Catch IOException: ", e);
        }

        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, USER_TOPIC);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.METADATA, metadata);
        configs.put(AWSSchemaRegistryConstants.TAGS, testTags);
    }

    private GlueSchemaRegistrySerializationFacade createGlueSerializationFacade(Map<String, Object> configs,
                                                                                SchemaByDefinitionFetcher schemaByDefinitionFetcher) {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .glueSchemaRegistryConfiguration(new GlueSchemaRegistryConfiguration(configs))
                        .credentialProvider(cred)
                        .schemaByDefinitionFetcher(schemaByDefinitionFetcher)
                        .build();

        return glueSchemaRegistrySerializationFacade;
    }

    /**
     * Tests serialization for generic record.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testSerialize_schemaParsing_succeeds(DataFormat dataFormat,
                                                     Object record) {
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);

        String schemaDefinition = glueSchemaRegistrySerializationFacade.getSchemaDefinition(dataFormat, record);

        when(mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(eq(schemaDefinition), eq(USER_SCHEMA), eq(dataFormat.name()),
                                                     anyMap())).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);
        UUID schemaVersionId = glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareInput(schemaDefinition, USER_SCHEMA, dataFormat.name()));

        assertNotNull(glueSchemaRegistrySerializationFacade.serialize(dataFormat, record, schemaVersionId));
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
    }

    @Test
    public void testSerialize_InvalidDataFormat_ThrowsException() {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);
        Exception exception = assertThrows(AWSSchemaRegistryException.class,
                                           () -> glueSchemaRegistrySerializationFacade.serialize(
                                                   DataFormat.UNKNOWN_TO_SDK_VERSION, genericAvroRecord,
                                                   SCHEMA_VERSION_ID_FOR_TESTING));
        assertTrue(exception.getMessage()
                           .contains("Unsupported data format:"));
    }

    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testSerialize_NullSchemaVersionId_ThrowsException(DataFormat dataFormat,
                                                                  Object record) {
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);
        assertThrows(IllegalArgumentException.class,
                     () -> glueSchemaRegistrySerializationFacade.serialize(dataFormat, record, null));
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
    }

    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION"})
    public void testSerialize_NullData_ThrowsException(DataFormat dataFormat) {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);
        assertThrows(IllegalArgumentException.class,
                     () -> glueSchemaRegistrySerializationFacade.serialize(dataFormat, null,
                                                                           SCHEMA_VERSION_ID_FOR_TESTING));
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade without configurations will throw out
     * AWSSchemaRegistryException.
     */
    @Test
    public void testBuildGSRSerializationFacade_nullConfig_throwsException() {
        Assertions.assertThrows(AWSSchemaRegistryException.class, () -> GlueSchemaRegistrySerializationFacade.builder()
                .configs(null)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build());
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade with null configurations but existing property.
     */
    @Test
    public void testBuildGSRSerializationFacade_nullConfigWithProp_throwsException() {
        Properties properties = new Properties();
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        properties.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, USER_TOPIC);

        Assertions.assertDoesNotThrow(() -> GlueSchemaRegistrySerializationFacade.builder()
                .configs(null)
                .credentialProvider(cred)
                .properties(properties)
                .build());
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade without configurations will throw out
     * AWSSchemaRegistryException.
     */
    @Test
    public void testBuildGSRSerializationFacade_nullCredentialProvider_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> GlueSchemaRegistrySerializationFacade.builder()
                .configs(configs)
                .credentialProvider(null)
                .build());
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade with invalid configurations will throw out
     * AWSSchemaRegistryException.
     */
    @Test
    public void testBuildGSRSerializationFacade_invalidConfigs_throwsException() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.UNKNOWN_TO_SDK_VERSION.toString());
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, USER_TOPIC);

        Assertions.assertThrows(AWSSchemaRegistryException.class, () -> GlueSchemaRegistrySerializationFacade.builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build());
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade with compression configuration
     */
    @ParameterizedTest
    @EnumSource(value = AWSSchemaRegistryConstants.COMPRESSION.class, names = {"NONE"}, mode = EnumSource.Mode.EXCLUDE)
    public void testBuildGSRSerializationFacade_withCompression_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);

        assertNotNull(glueSchemaRegistrySerializationFacade);
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
    }

    @Test
    public void testInitialize_nullCredentials_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> GlueSchemaRegistrySerializationFacade.builder()
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .glueSchemaRegistryConfiguration(new GlueSchemaRegistryConfiguration(configs))
                .build());
    }

    /**
     * Tests serialization with null topic.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testSerialize_nullTopic_succeeds(DataFormat dataFormat,
                                                 Object record,
                                                 AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());

        String schemaDefinition = glueSchemaRegistrySerializerFactory.getInstance(dataFormat,
                                                                                  new GlueSchemaRegistryConfiguration(
                                                                                          configs))
                .getSchemaDefinition(record);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                initializeGSRKafkaSerializer(configs, schemaDefinition, mockSchemaByDefinitionFetcher, SCHEMA_VERSION_ID_FOR_TESTING);

        String nullTopic = null;
        byte[] serializedData = glueSchemaRegistryKafkaSerializer.serialize(nullTopic, record);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
    }

    /**
     * Tests serialization.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testSerialize_enums_succeeds(DataFormat dataFormat,
                                             Object record,
                                             AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());

        String schemaDefinition = glueSchemaRegistrySerializerFactory.getInstance(dataFormat,
                                                                                  new GlueSchemaRegistryConfiguration(
                                                                                          configs))
                .getSchemaDefinition(record);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                initializeGSRKafkaSerializer(configs, schemaDefinition, mockSchemaByDefinitionFetcher, SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
    }

    /**
     * Tests serialization for invalid data will throw out AWSSchemaRegistryException.
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    public void testSerialize_invalidData_throwsException(DataFormat dataFormat,
                                                          Object record,
                                                          AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        String schemaDefinition = glueSchemaRegistrySerializerFactory.getInstance(dataFormat,
                                                                                  new GlueSchemaRegistryConfiguration(
                                                                                          configs))
                .getSchemaDefinition(record);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                initializeGSRKafkaSerializer(configs, schemaDefinition, mockSchemaByDefinitionFetcher, SCHEMA_VERSION_ID_FOR_TESTING);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                                () -> glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record));
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
    }

    /**
     * Tests serialization for malformed JSON Schema will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testSerialize_malformedJsonSchema_throwsException() {
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        JsonDataWithSchema record = RecordGenerator.createRecordWithMalformedJsonSchema();

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                initializeGSRKafkaSerializer(configs, "fakeSchemaDef", mockSchemaByDefinitionFetcher, SCHEMA_VERSION_ID_FOR_TESTING);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                                () -> glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record));
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
    }

    /**
     * Tests serialization for malformed JSON data will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testSerialize_malformedJsonData_throwsException() {
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        JsonDataWithSchema record = RecordGenerator.createRecordWithMalformedJsonData();

        String schemaDefinition = glueSchemaRegistrySerializerFactory.getInstance(DataFormat.JSON,
                                                                                  new GlueSchemaRegistryConfiguration(
                                                                                          configs))
                .getSchemaDefinition(record);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                initializeGSRKafkaSerializer(configs, schemaDefinition, mockSchemaByDefinitionFetcher, SCHEMA_VERSION_ID_FOR_TESTING);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                                () -> glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record));
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
    }

    /**
     * Tests serialization with unsupported protocol will throw out AWSSchemaRegistryException.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat.class, names = {"AVRO"}, mode = EnumSource.Mode.INCLUDE)
    public void testSerialize_unsupportedProtocolMessage_throwsException(DataFormat dataFormat) {
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
        ArrayList<Integer> unSupportedFormatArray = new ArrayList<>();
        unSupportedFormatArray.add(1);

        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                initializeGSRKafkaSerializer(configs, null, mockSchemaByDefinitionFetcher, null);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                                () -> glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, unSupportedFormatArray));
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
    }

    /**
     * Tests invoking shutdown invokes close method.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat.class, names = {"UNKNOWN_TO_SDK_VERSION"}, mode = EnumSource.Mode.EXCLUDE)
    public void testClose_succeeds(DataFormat dataFormat) {
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                mock(GlueSchemaRegistrySerializationFacade.class);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                new GlueSchemaRegistryKafkaSerializer(cred, configs);
        glueSchemaRegistryKafkaSerializer.setGlueSchemaRegistrySerializationFacade(
                glueSchemaRegistrySerializationFacade);

        glueSchemaRegistryKafkaSerializer.close();
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
    }

    /**
     * Tests serialize to check if data reduces after compression.
     */
    @ParameterizedTest
    @EnumSource(value = AWSSchemaRegistryConstants.COMPRESSION.class, names = {"NONE"}, mode = EnumSource.Mode.EXCLUDE)
    public void testSerialize_arraysWithCompression_byteArraySizeIsReduced(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws IOException {
        int capacity = 1000000;
        Schema schema = getSchema(AVRO_USER_ARRAY_STRING_SCHEMA_FILE);
        GenericData.Array<String> array = new GenericData.Array<>(capacity, schema);
        for (int i = 0; i < capacity; i++) {
            array.add("test");
        }

        String schemaDefinition = AVROUtils.getInstance()
                .getSchemaDefinition(array);
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        Map<String, Object> configsWithCompressionEnabled = configs.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        configsWithCompressionEnabled.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        GlueSchemaRegistryKafkaSerializer gsrKafkaSerializerWithoutCompression =
                initializeGSRKafkaSerializer(configs, schemaDefinition, mockSchemaByDefinitionFetcher, SCHEMA_VERSION_ID_FOR_TESTING);
        GlueSchemaRegistryKafkaSerializer gsrKafkaSerializerWithCompression =
                initializeGSRKafkaSerializer(configsWithCompressionEnabled, schemaDefinition,
                    mockSchemaByDefinitionFetcher, SCHEMA_VERSION_ID_FOR_TESTING);
        byte[] serializedData = gsrKafkaSerializerWithoutCompression.serialize(TEST_TOPIC, array);
        byte[] compressedAndSerializedData = gsrKafkaSerializerWithCompression.serialize(TEST_TOPIC, array);

        assertTrue(serializedData.length > compressedAndSerializedData.length);
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
    }

    /**
     * Tests registerSchemaVersion method of Serializer with metadata configuration
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    public void testSerializer_registerSchemaVersion_withMetadataConfig_succeeds(DataFormat dataFormat,
                                                                                 Object record) {
        GlueSchemaRegistrySerializationFacade glueSerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);
        String schemaDefinition = glueSerializationFacade.getSchemaDefinition(dataFormat, record);
        Map<String, String> metadata = getMetadata();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, TRANSPORT_NAME);

        when(mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(schemaDefinition, USER_SCHEMA, dataFormat.name(),
                                                     metadata)).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);

        UUID schemaVersionId = glueSerializationFacade.getOrRegisterSchemaVersion(
                prepareInput(schemaDefinition, USER_SCHEMA, dataFormat.name()));
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId);
    }

    /**
     * Tests registerSchemaVersion method of Serializer without metadata configuration
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    public void testSerializer_registerSchemaVersion_withoutMetadataConfig_succeeds(DataFormat dataFormat,
                                                                                    Object record) {
        configs.remove(AWSSchemaRegistryConstants.METADATA);
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);
        Map<String, String> metadata = new HashMap<>();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, TRANSPORT_NAME);

        String schemaDefinition = glueSchemaRegistrySerializationFacade.getSchemaDefinition(dataFormat, record);
        when(mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(schemaDefinition, USER_SCHEMA, dataFormat.name(),
                                                     metadata)).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);

        UUID schemaVersionId = glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareInput(schemaDefinition, USER_SCHEMA, dataFormat.name()));
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId);
    }

    /**
     * Tests registerSchemaVersion method of Serializer when PutSchemaVersionMetadata API throws exception
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    public void testSerializer_registerSchemaVersion_whenPutSchemaVersionMetadataThrowsException(DataFormat dataFormat,
                                                                                                 Object record) {
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        AWSSchemaRegistryClient awsSchemaRegistryClient =
                new AWSSchemaRegistryClient(cred, glueSchemaRegistryConfiguration);
        AWSSchemaRegistryClient spyClient = spy(awsSchemaRegistryClient);
        SchemaByDefinitionFetcher schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(spyClient, glueSchemaRegistryConfiguration);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSerializationFacade(configs, schemaByDefinitionFetcher);

        String schemaDefinition = glueSchemaRegistrySerializationFacade.getSchemaDefinition(dataFormat, record);

        Map<String, String> metadata = getMetadata();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, TRANSPORT_NAME);

        EntityNotFoundException entityNotFoundException = EntityNotFoundException.builder()
                .message(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG)
                .build();
        AWSSchemaRegistryException awsSchemaRegistryException = new AWSSchemaRegistryException(entityNotFoundException);
        doThrow(awsSchemaRegistryException).when(spyClient)
                .getSchemaVersionIdByDefinition(schemaDefinition, USER_SCHEMA, dataFormat.name());

        GetSchemaVersionResponse getSchemaVersionResponse =
                createGetSchemaVersionResponse(SCHEMA_VERSION_ID_FOR_TESTING, schemaDefinition, dataFormat.name());
        doReturn(getSchemaVersionResponse).when(spyClient)
                .registerSchemaVersion(schemaDefinition, USER_SCHEMA, dataFormat.name());

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            MetadataKeyValuePair metadataKeyValuePair = createMetadataKeyValuePair(entry);
            doThrow(new AWSSchemaRegistryException("Put schema version metadata failed.")).when(spyClient)
                    .putSchemaVersionMetadata(SCHEMA_VERSION_ID_FOR_TESTING, metadataKeyValuePair);
        }
        doNothing().when(spyClient)
                .putSchemaVersionMetadata(SCHEMA_VERSION_ID_FOR_TESTING, metadata);

        UUID schemaVersionId = glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareInput(schemaDefinition, USER_SCHEMA, dataFormat.name()));
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId);
    }

    /**
     * Tests registerSchemaVersion method of Serializer with custom jackson configuration
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testRegisterSchemaVersion_withCustomJacksonConfiguration_succeeds(DataFormat dataFormat,
                                                                                  Object record,
                                                                                  AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        List<String> jacksonSerializationFeatures =
                Arrays.asList(SerializationFeature.FLUSH_AFTER_WRITE_VALUE.name());
        configs.put(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES,
                    jacksonSerializationFeatures);
        List<String> jacksonDeserializationFeatures =
                Arrays.asList(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS.name());
        configs.put(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES,
                    jacksonDeserializationFeatures);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name());

        String schemaDefinition = glueSchemaRegistrySerializerFactory.getInstance(dataFormat,
                                                                                  new GlueSchemaRegistryConfiguration(
                                                                                          configs))
                .getSchemaDefinition(record);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                initializeGSRKafkaSerializer(configs, schemaDefinition, mockSchemaByDefinitionFetcher, SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT);
        configs.remove(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES);
        configs.remove(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES);
    }

    /**
     * Tests the encode method.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testEncode_WhenValidInputIsPassed_EncodesTheBytes(DataFormat dataFormat,
                                                                  Object record) throws Exception {
        GlueSchemaRegistryDataFormatSerializer glueSchemaRegistrySerializer =
            glueSchemaRegistrySerializerFactory.getInstance(dataFormat,
            new GlueSchemaRegistryConfiguration(configs));

        String schemaDefinition = glueSchemaRegistrySerializer.getSchemaDefinition(record);
        byte[] payload = glueSchemaRegistrySerializer.serialize(record);

        com.amazonaws.services.schemaregistry.common.Schema schema =
                new com.amazonaws.services.schemaregistry.common.Schema(schemaDefinition, dataFormat.name(), TEST_SCHEMA);

        Map<String, String> metadata = getMetadata();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, TRANSPORT_NAME);
        when(mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(schemaDefinition, TEST_SCHEMA, dataFormat.name(),
                                                     metadata)).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);

        //Test subject
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.encode(TRANSPORT_NAME, schema, payload);

        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING,
                              AWSSchemaRegistryConstants.COMPRESSION.NONE, payload);
    }

    private AWSSerializerInput prepareInput(String schemaDefinition,
                                            String schemaName,
                                            String dataFormat) {
        return AWSSerializerInput.builder()
                .schemaDefinition(schemaDefinition)
                .schemaName(schemaName)
                .dataFormat(dataFormat)
                .build();
    }

    private AWSSerializerInput prepareInputV2(String schemaDefinition,
                                            String schemaName,
                                            String dataFormat,
                                            Compatibility compatibility) {
        return AWSSerializerInput.builder()
                .schemaDefinition(schemaDefinition)
                .schemaName(schemaName)
                .dataFormat(dataFormat)
                .compatibility(compatibility)
                .build();
    }

    @Test
    public void testRegisterSchema_nullSerializerInput_throwsException() {
        GlueSchemaRegistrySerializationFacade glueSerializationFacade =
                createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher);
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> glueSerializationFacade.getOrRegisterSchemaVersion(null));
    }

    private Map<String, String> getMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("event-source-1", "topic1");
        metadata.put("event-source-2", "topic2");
        metadata.put("event-source-3", "topic3");
        metadata.put("event-source-4", "topic4");
        metadata.put("event-source-5", "topic5");

        return metadata;
    }

    private GetSchemaVersionResponse createGetSchemaVersionResponse(UUID schemaVersionId,
                                                                    String schemaDefinition,
                                                                    String dataFormat) {
        return GetSchemaVersionResponse.builder()
                .schemaVersionId(schemaVersionId.toString())
                .schemaDefinition(schemaDefinition)
                .dataFormat(dataFormat)
                .build();
    }

    private MetadataKeyValuePair createMetadataKeyValuePair(Map.Entry<String, String> metadataEntry) {
        return MetadataKeyValuePair.builder()
                .metadataKey(metadataEntry.getKey())
                .metadataValue(metadataEntry.getValue())
                .build();
    }
}

class Customer {
    String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
