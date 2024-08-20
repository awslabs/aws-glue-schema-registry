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

import com.amazonaws.services.schemaregistry.common.*;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.avro.User;
import com.amazonaws.services.schemaregistry.serializers.json.Car;
import com.amazonaws.services.schemaregistry.serializers.json.Employee;
import com.amazonaws.services.schemaregistry.serializers.json.JsonSerializer;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import com.amazonaws.services.schemaregistry.utils.SchemaLoader;
import com.amazonaws.services.schemaregistry.utils.SerializedByteArrayGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.cache.LoadingCache;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing protocol agnostic de-serializer.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class GlueSchemaRegistryDeserializationFacadeTest {
    public static final String AVRO_USER_SCHEMA_FILE = "src/test/resources/avro/user.avsc";
    public static final String AVRO_EMP_RECORD_SCHEMA_FILE_PATH = "src/test/resources/avro/emp_record.avsc";
    private static final String TEST_SCHEMA_NAME = "Test";
    private static final String TEST_SCHEMA_ARN =
            "arn:aws:glue:ca-central-1:111111111111:schema/registry_name/test_schema";
    private static final String USER_SCHEMA_NAME = "User";
    private static final UUID USER_SCHEMA_VERSION_ID = UUID.randomUUID();
    private static final String USER_SCHEMA_ARN =
            "arn:aws:glue:ca-central-1:111111111111:schema/registry_name" + "/user_schema";
    private static final String EMPLOYEE_SCHEMA_NAME = "Employee";
    private static final UUID EMPLOYEE_SCHEMA_VERSION_ID = UUID.randomUUID();
    private static final String EMPLOYEE_SCHEMA_ARN =
            "arn:aws:glue:ca-central-1:111111111111:schema/registry_name" + "/employee_schema";

    private static final AVROUtils AVRO_UTILS = AVROUtils.getInstance();
    private static final GenericRecord genericAvroRecord = RecordGenerator.createGenericAvroRecord();
    private static final GenericData.EnumSymbol genericUserEnumAvroRecord =
            RecordGenerator.createGenericUserEnumAvroRecord();
    private static final GenericData.Array<Integer> genericIntArrayAvroRecord =
            RecordGenerator.createGenericIntArrayAvroRecord();
    private static final GenericData.Array<String> genericStringArrayAvroRecord =
            RecordGenerator.createGenericStringArrayAvroRecord();
    private static final GenericData.Record genericUserMapAvroRecord = RecordGenerator.createGenericUserMapAvroRecord();
    private static final GenericData.Record genericUserUnionAvroRecord =
            RecordGenerator.createGenericUserUnionAvroRecord();
    private static final GenericData.Record genericUserUnionNullAvroRecord =
            RecordGenerator.createGenericUnionWithNullValueAvroRecord();
    private static final GenericData.Fixed genericFixedAvroRecord = RecordGenerator.createGenericFixedAvroRecord();
    private static final GenericData.Record genericMultipleTypesAvroRecord =
            RecordGenerator.createGenericMultipleTypesAvroRecord();
    private static final Car specificJsonCarRecord = RecordGenerator.createSpecificJsonRecord();
    private static final Employee specificJsonEmpployeeRecord = RecordGenerator.createInvalidEmployeeJsonRecord();
    private static final User userDefinedPojoAvro = RecordGenerator.createSpecificAvroRecord();
    private static GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration =
            new GlueSchemaRegistryConfiguration(new HashMap<String, Object>() {{
                put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
            }});
    private static final JsonSerializer JSON_SERIALIZER = new JsonSerializer(glueSchemaRegistryConfiguration);
    private static GenericRecord genericUserAvroRecord;
    private static org.apache.avro.Schema userAvroSchema;
    private static String userSchemaDefinition;
    private static GetSchemaVersionResponse userSchemaVersionResponse;
    private static GenericRecord genericEmployeeAvroRecord;
    private static org.apache.avro.Schema employeeAvroSchema;
    private static String employeeSchemaDefinition;
    private static GetSchemaVersionResponse employeeSchemaVersionResponse;
    private static GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade;
    private static GlueSchemaRegistrySerializationFacade compressingGlueSchemaRegistrySerializationFacade;
    private final Map<String, Object> configs = new HashMap<>();
    @Mock
    private SchemaByDefinitionFetcher mockSchemaByDefinitionFetcher;

    @Mock
    private AWSSchemaRegistryClient mockDefaultRegistryClient;
    @Mock
    private AWSSchemaRegistryClient mockClientThatThrowsException;
    @Mock
    private AWSSchemaRegistryClient mockSchemaRegistryClient;
    @Mock
    private AwsCredentialsProvider mockDefaultCredProvider;
    @Mock
    private GlueSchemaRegistryDataFormatDeserializer mockDataFormatDeserializer;
    @Mock
    private GlueSchemaRegistryDeserializerFactory mockDeserializerFactory;

    private static List<Arguments> testDataAndSchemaProvider() {
        List<Object> genericAvroRecords =
                Arrays.asList(genericAvroRecord, genericUserEnumAvroRecord, genericIntArrayAvroRecord,
                              genericStringArrayAvroRecord, genericUserMapAvroRecord, genericUserUnionAvroRecord,
                              genericUserUnionNullAvroRecord, genericFixedAvroRecord, genericMultipleTypesAvroRecord);

        List<Object> specificAvroRecords = Arrays.asList(userDefinedPojoAvro);

        List<Object> wrapperJsonRecords = Arrays.stream(RecordGenerator.TestJsonRecord.values())
                .filter(RecordGenerator.TestJsonRecord::isValid)
                .map(RecordGenerator::createGenericJsonRecord)
                .collect(Collectors.toList());

        List<Object> specificJsonRecords = Arrays.asList(specificJsonCarRecord);

        AWSSchemaRegistryConstants.COMPRESSION[] compressions = AWSSchemaRegistryConstants.COMPRESSION.values();

        List<Arguments> args = new ArrayList<>();

        for (AWSSchemaRegistryConstants.COMPRESSION compression : compressions) {
            args.addAll(genericAvroRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.AVRO, r, AVRO_UTILS.getSchemaDefinition(r),
                                                              UUID.randomUUID(),
                                                              AvroRecordType.GENERIC_RECORD.getName(), compression,
                                                              getAvroBytes(r, AVRO_UTILS.getSchemaDefinition(r))))
                                .collect(Collectors.toList()));
            args.addAll(specificAvroRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.AVRO, r, AVRO_UTILS.getSchemaDefinition(r),
                                                              UUID.randomUUID(),
                                                              AvroRecordType.SPECIFIC_RECORD.getName(), compression,
                                                              getAvroBytes(r, AVRO_UTILS.getSchemaDefinition(r))))
                                .collect(Collectors.toList()));
            args.addAll(wrapperJsonRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.JSON, r,
                                                              JSON_SERIALIZER.getSchemaDefinition(r), UUID.randomUUID(),
                                                              AvroRecordType.GENERIC_RECORD.getName(), // Not being used
                                                              compression, JSON_SERIALIZER.serialize(r)))
                                .collect(Collectors.toList()));
            args.addAll(specificJsonRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.JSON, r,
                                                              JSON_SERIALIZER.getSchemaDefinition(r), UUID.randomUUID(),
                                                              AvroRecordType.GENERIC_RECORD.getName(), // Not being used
                                                              compression, JSON_SERIALIZER.serialize(r)))
                                .collect(Collectors.toList()));
        }

        return args;
    }

    private static List<Arguments> testInvalidDataAndSchemaProvider() {
        List<Object> specificJsonRecords = Arrays.asList(specificJsonEmpployeeRecord);

        AWSSchemaRegistryConstants.COMPRESSION[] compressions = AWSSchemaRegistryConstants.COMPRESSION.values();

        List<Arguments> args = new ArrayList<>();

        for (AWSSchemaRegistryConstants.COMPRESSION compression : compressions) {
            args.addAll(specificJsonRecords.stream()
                                .map(r -> Arguments.arguments(DataFormat.JSON, r,
                                                              JSON_SERIALIZER.getSchemaDefinition(r), UUID.randomUUID(),
                                                              AvroRecordType.GENERIC_RECORD.getName(), // Not being used
                                                              compression, JSON_SERIALIZER.serialize(r)))
                                .collect(Collectors.toList()));
        }

        return args;
    }

    private static byte[] getAvroBytes(Object record,
                                       String inputSchemaDefinition) {
        ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
        GenericDatumWriter writer =
                new GenericDatumWriter<GenericRecord>(AVRO_UTILS.parseSchema(inputSchemaDefinition));
        BinaryEncoder encoder = EncoderFactory.get()
                .directBinaryEncoder(expectedBytes, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            fail("Unable to get bytes from record.");
        }
        return expectedBytes.toByteArray();
    }

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    public void setup() {
        this.configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        this.configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        when(mockClientThatThrowsException.getSchemaVersionResponse(Mockito.anyString())).thenThrow(
                new AWSSchemaRegistryException("some runtime exception"));

        glueSchemaRegistrySerializationFacade = GlueSchemaRegistrySerializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(configs)
                .schemaByDefinitionFetcher(this.mockSchemaByDefinitionFetcher)
                .build();

        Map<String, Object> compressionConfig = new HashMap<>();
        compressionConfig.putAll(this.configs);
        compressionConfig.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE,
                              AWSSchemaRegistryConstants.COMPRESSION.ZLIB.toString());

        compressingGlueSchemaRegistrySerializationFacade = GlueSchemaRegistrySerializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(compressionConfig)
                .schemaByDefinitionFetcher(this.mockSchemaByDefinitionFetcher)
                .build();

        genericUserAvroRecord = RecordGenerator.createGenericAvroRecord();
        userAvroSchema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        userSchemaDefinition = AVRO_UTILS.getSchemaDefinition(genericUserAvroRecord);
        userSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(userAvroSchema.toString())
                .dataFormat(DataFormat.AVRO)
                .schemaArn(USER_SCHEMA_ARN)
                .build();

        genericEmployeeAvroRecord = RecordGenerator.createGenericEmpRecord();
        employeeAvroSchema = SchemaLoader.loadAvroSchema(AVRO_EMP_RECORD_SCHEMA_FILE_PATH);
        employeeSchemaDefinition = AVRO_UTILS.getSchemaDefinition(genericEmployeeAvroRecord);
        employeeSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(employeeAvroSchema.toString())
                .dataFormat(DataFormat.AVRO)
                .schemaArn(EMPLOYEE_SCHEMA_ARN)
                .build();

        when(mockSchemaRegistryClient.getSchemaVersionResponse(
                Mockito.eq(USER_SCHEMA_VERSION_ID.toString()))).thenReturn(userSchemaVersionResponse);
        when(mockSchemaRegistryClient.getSchemaVersionResponse(
                Mockito.eq(EMPLOYEE_SCHEMA_VERSION_ID.toString()))).thenReturn(employeeSchemaVersionResponse);

        when(mockDataFormatDeserializer.deserialize(Mockito.any(ByteBuffer.class),
                                                    Mockito.eq(new com.amazonaws.services.schemaregistry.common.Schema(
                                                            employeeAvroSchema.toString(), DataFormat.AVRO.name(), "employee_schema")))).thenReturn(
                genericEmployeeAvroRecord);
        when(mockDataFormatDeserializer.deserialize(Mockito.any(ByteBuffer.class),
                                                    Mockito.eq(new com.amazonaws.services.schemaregistry.common.Schema(
                                                            userAvroSchema.toString(), DataFormat.AVRO.name(), "user_schema")))).thenReturn(
                genericUserAvroRecord);

        when(mockDeserializerFactory.getInstance(Mockito.any(DataFormat.class),
                                                 Mockito.any(GlueSchemaRegistryConfiguration.class))).thenReturn(
                mockDataFormatDeserializer);

        when(mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(Mockito.eq(userSchemaDefinition),
                                                                    Mockito.eq(USER_SCHEMA_NAME),
                                                                    Mockito.eq(DataFormat.AVRO.name()),
                                                                    Mockito.anyMap())).thenReturn(
                USER_SCHEMA_VERSION_ID);
        when(mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(Mockito.eq(employeeSchemaDefinition),
                                                                    Mockito.eq(EMPLOYEE_SCHEMA_NAME),
                                                                    Mockito.eq(DataFormat.AVRO.name()),
                                                                    Mockito.anyMap())).thenReturn(
                EMPLOYEE_SCHEMA_VERSION_ID);
    }

    /**
     * Tests the GlueSchemaRegistryemployeeDeserializationFacade instantiation when an no configuration is provided.
     */
    @Test
    public void testBuildDeserializer_withNoArguments_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> GlueSchemaRegistryDeserializationFacade.builder()
                .build());
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade instantiation when an no configuration is provided.
     */
    @Test
    public void testBuildDeserializer_withNullConfig_throwsException() {
        assertThrows(IllegalArgumentException.class,
                     () -> new GlueSchemaRegistryDeserializationFacade(null, DefaultCredentialsProvider.create()));
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade and assert for dependency values with config map.
     */
    @Test
    public void testBuildDeserializer_withConfigs_buildsSuccessfully() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .credentialProvider(this.mockDefaultCredProvider)
                        .schemaRegistryClient(this.mockDefaultRegistryClient)
                        .configs(this.configs)
                        .build();

        assertEquals(this.mockDefaultCredProvider, glueSchemaRegistryDeserializationFacade.getCredentialsProvider());
        assertEquals(new GlueSchemaRegistryConfiguration(this.configs),
                     glueSchemaRegistryDeserializationFacade.getGlueSchemaRegistryConfiguration());
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade and assert for dependency values with properties.
     */
    @Test
    public void testBuildDeserializer_withProperties_buildsSuccessfully() {
        Properties props = new Properties();
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");

        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .credentialProvider(this.mockDefaultCredProvider)
                        .schemaRegistryClient(this.mockDefaultRegistryClient)
                        .properties(props)
                        .build();

        assertEquals(this.mockDefaultCredProvider, glueSchemaRegistryDeserializationFacade.getCredentialsProvider());
        assertEquals(this.mockDefaultRegistryClient, glueSchemaRegistryDeserializationFacade.getSchemaRegistryClient());
        assertEquals(new GlueSchemaRegistryConfiguration(props),
                     glueSchemaRegistryDeserializationFacade.getGlueSchemaRegistryConfiguration());
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade instantiation when an empty configuration is provided.
     */
    @Test
    public void testBuildDeserializer_emptyConfig_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(new HashMap<>())
                .build());
        assertThrows(IllegalArgumentException.class, () -> GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .properties(new Properties())
                .build());
        assertThrows(IllegalArgumentException.class, () -> GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(new HashMap<>())
                .schemaRegistryClient(this.mockDefaultRegistryClient)
                .build());
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade instantiation when an invalid configuration is provided.
     */
    @Test
    public void testBuildDeserializer_badConfig_throwsException() {
        Map<String, ?> badConfig = new HashMap<String, String>() {{
            put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.UNKNOWN_TO_SDK_VERSION.toString());
        }};
        assertThrows(AWSSchemaRegistryException.class, () -> GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(badConfig)
                .build());
        assertThrows(AWSSchemaRegistryException.class, () -> GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(badConfig)
                .schemaRegistryClient(this.mockDefaultRegistryClient)
                .build());
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade instantiation when an null configuration is provided.
     */
    @Test
    public void testBuildDeserializer_nullConfig_throwsException() {
        assertThrows(AWSSchemaRegistryException.class, () -> GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(null)
                .build());
        Exception exception = assertThrows(AWSSchemaRegistryException.class,
                                           () -> GlueSchemaRegistryDeserializationFacade.builder()
                                                   .credentialProvider(this.mockDefaultCredProvider)
                                                   .configs(null)
                                                   .schemaRegistryClient(this.mockDefaultRegistryClient)
                                                   .build());

        assertEquals("Either properties or configuration has to be provided", exception.getMessage());
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade by retrieving schema definition - positive case.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testGetSchemaDefinition_getSchemaVersionFromClientSucceeds_schemaDefinitionMatches(DataFormat dataFormat,
                                                                                                   Object record,
                                                                                                   String inputSchemaDefinition,
                                                                                                   UUID schemaVersionId,
                                                                                                   AvroRecordType avroRecordType,
                                                                                                   AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        byte[] serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId);

        GetSchemaVersionResponse schemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build();

        when(mockSchemaRegistryClient.getSchemaVersionResponse(Mockito.eq(schemaVersionId.toString()))).thenReturn(
                schemaVersionResponse);

        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade(mockSchemaRegistryClient);
        String schemaDefinition = glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData);
        assertEquals(inputSchemaDefinition, schemaDefinition);
        // Clean-up
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
    }

    /**
     * Tests the DeserializationFacade by retrieving schema definition - negative case.
     * Tests the GlueSchemaRegistryDeserializationFacade by retrieving schema definition - negative case.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testGetSchemaDefinition_getSchemaVersionFromClientFails_throwsException(DataFormat dataFormat,
                                                                                        Object record,
                                                                                        String inputSchemaDefinition,
                                                                                        UUID schemaVersionId) {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade(mockClientThatThrowsException);
        byte[] serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId);
        ;
        assertThrows(AWSSchemaRegistryException.class,
                     () -> glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData));
    }

    /**
     * Tests the getSchemaVersionId for exception case where data length is invalid.
     */
    @Test
    public void testGetSchemaDefinition_invalidDataLength_throwsException() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade();
        byte[] serializedData =
                new byte[]{AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, AWSSchemaRegistryConstants.COMPRESSION_BYTE};
        assertThrows(GlueSchemaRegistryIncompatibleDataException.class,
                     () -> glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData));
    }

    /**
     * Tests the getSchemaVersionId for exception case where the header version byte is unknown.
     */
    @Test
    public void testGetSchemaDefinition_invalidHeaderVersionByte_throwsException() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade();
        byte[] serializedData = SerializedByteArrayGenerator.constructBasicSerializedData((byte) 99,
                                                                                          AWSSchemaRegistryConstants.COMPRESSION_BYTE,
                                                                                          UUID.randomUUID());
        assertThrows(GlueSchemaRegistryIncompatibleDataException.class,
                     () -> glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData));
    }

    /**
     * Tests the getSchemaVersionId for exception case where the compression byte is unknown.
     */
    @Test
    public void testGetSchemaDefinition_invalidCompressionByte_throwsException() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade();
        byte[] serializedData = SerializedByteArrayGenerator.constructBasicSerializedData(
                AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, (byte) 99, UUID.randomUUID());

        assertThrows(GlueSchemaRegistryIncompatibleDataException.class,
                     () -> glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData));
    }

    /**
     * Tests the getSchemaVersionId for exception case where the buffer is null.
     */
    @Test
    public void testGetSchemaDefinition_nullBuffer_throwsException() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade();
        assertThrows(IllegalArgumentException.class,
                     () -> glueSchemaRegistryDeserializationFacade.getSchemaDefinition((ByteBuffer) null));
    }

    /**
     * Tests the getSchemaVersionId for exception case where the byte array is null.
     */
    @Test
    public void testGetSchemaDefinition_nullByte_throwsException() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade();
        assertThrows(IllegalArgumentException.class,
                     () -> glueSchemaRegistryDeserializationFacade.getSchemaDefinition((byte[]) null));
    }

    /**
     * Tests the de-serialization positive case.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testDeserialize_withValidSchemaResponse_recordMatches(DataFormat dataFormat,
                                                                      Object record,
                                                                      String inputSchemaDefinition,
                                                                      UUID schemaVersionId,
                                                                      String avroRecordType,
                                                                      AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, avroRecordType);
        byte[] serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId);

        GetSchemaVersionResponse schemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build();

        when(mockSchemaRegistryClient.getSchemaVersionResponse(Mockito.eq(schemaVersionId.toString()))).thenReturn(
                schemaVersionResponse);

        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade(mockSchemaRegistryClient);

        Object deserializedObject =
                glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData));

        // AVRO converts strings into org.apache.avro.util.Utf8 char sequences and equals of it does not work
        // with java.lang.String
        // hence using toString() so that equality checks for avro too pass
        // https://stackoverflow.com/questions/15690997/avro-and-java-deserialized-map-of-string-doesnt-equals
        // -original-map
        assertEquals(record.toString(), deserializedObject.toString());


        configs.remove(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE);
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
    }

    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testDeserialize_withSerdeConfigs_recordMatches(DataFormat dataFormat,
                                                               Object record,
                                                               String inputSchemaDefinition,
                                                               UUID schemaVersionId,
                                                               String avroRecordType,
                                                               AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, avroRecordType);
        configs.put(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES,
                    Arrays.asList(DeserializationFeature.EAGER_DESERIALIZER_FETCH.name()));
        byte[] serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId);

        GetSchemaVersionResponse schemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build();

        when(mockSchemaRegistryClient.getSchemaVersionResponse(Mockito.eq(schemaVersionId.toString()))).thenReturn(
                schemaVersionResponse);

        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade(mockSchemaRegistryClient);

        Object deserializedObject =
                glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData));

        // AVRO converts strings into org.apache.avro.util.Utf8 char sequences and equals of it does not work
        // with java.lang.String
        // hence using toString() so that equality checks for avro too pass
        // https://stackoverflow.com/questions/15690997/avro-and-java-deserialized-map-of-string-doesnt-equals
        // -original-map
        assertEquals(record.toString(), deserializedObject.toString());


        configs.remove(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE);
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
        configs.remove(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES);
    }

    /**
     * Tests the de-serialization of specific json record with wrong classname
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    public void testDeserialize_invalidSpecificJsonRecord_throwsException(DataFormat dataFormat,
                                                                          Object record,
                                                                          String inputSchemaDefinition,
                                                                          UUID schemaVersionId,
                                                                          String avroRecordType,
                                                                          AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        byte[] serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId);

        GetSchemaVersionResponse schemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build();

        when(mockSchemaRegistryClient.getSchemaVersionResponse(Mockito.eq(schemaVersionId.toString()))).thenReturn(
                schemaVersionResponse);

        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade(mockSchemaRegistryClient);

        assertThrows(AWSSchemaRegistryException.class, () -> glueSchemaRegistryDeserializationFacade.deserialize(
                prepareDeserializerInput(serializedData)));

        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
    }

    /**
     * Tests the de-serialization of multiple records of different schemas.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION", "JSON", "PROTOBUF"})
    public void testDeserialize_withMultipleRecords_recordsMatch(DataFormat dataFormat) {
        byte[] serializedUserData = createSerializedUserData(genericUserAvroRecord, dataFormat);
        byte[] serializedEmployeeData = createSerializedEmployeeData(genericEmployeeAvroRecord, dataFormat);

        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade(mockDeserializerFactory);
        Object deserializedUserObject =
                glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedUserData));
        Object deserializedEmployeeObject =
                glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedEmployeeData));

        assertEquals(genericUserAvroRecord, deserializedUserObject);
        assertEquals(genericEmployeeAvroRecord, deserializedEmployeeObject);
    }

    /**
     * Tests the de-serialization negative case UnknownDataException.
     */
    @Test
    public void testDeserialize_invalidData_throwsException() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade();
        byte[] serializedData =
                new byte[]{AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, AWSSchemaRegistryConstants.COMPRESSION_BYTE};
        assertThrows(GlueSchemaRegistryIncompatibleDataException.class,
                     () -> glueSchemaRegistryDeserializationFacade.deserialize(
                             prepareDeserializerInput(serializedData)));
    }

    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    public void testGetActualData_withValidBytes_ReturnsActualBytes(DataFormat dataFormat,
                                                                    Object record,
                                                                    String inputSchemaDefinition,
                                                                    UUID schemaVersionId,
                                                                    String avroRecordType,
                                                                    AWSSchemaRegistryConstants.COMPRESSION compressionType,
                                                                    byte[] bytes) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        byte[] serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId);

        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade(mockSchemaRegistryClient);

        byte[] actualBytes = glueSchemaRegistryDeserializationFacade.getActualData(serializedData);

        //Convert to ByteBuffer and compare.
        assertEquals(ByteBuffer.wrap(bytes), ByteBuffer.wrap(actualBytes));

        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
    }

    /**
     * Tests the de-serialization for exception case where the deserializer input is null.
     */
    @Test
    public void testDeserialize_nullDeserializerInput_throwsException() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade();
        assertThrows(IllegalArgumentException.class, () -> glueSchemaRegistryDeserializationFacade.deserialize(null));
    }

    /**
     * Tests the deserialization case where retrieved schema data is stored in cache
     */
    @Test
    public void testDeserializer_retrieveSchemaRegistryMetadata_MetadataIsCached() throws InterruptedException {

        String dataFormat = DataFormat.AVRO.name();
        String inputSchemaDefinition = userSchemaDefinition;
        UUID schemaVersionId = USER_SCHEMA_VERSION_ID;

        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, AWSSchemaRegistryConstants.COMPRESSION.NONE.name());
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.name());

        byte[] serializedData = createSerializedData(genericUserAvroRecord, DataFormat.valueOf(dataFormat), inputSchemaDefinition, schemaVersionId);

        GetSchemaVersionResponse schemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build();

        //Mock to return success and failures.
        when(mockSchemaRegistryClient.getSchemaVersionResponse(Mockito.eq(schemaVersionId.toString())))
            .thenReturn(schemaVersionResponse)
            .thenReturn(schemaVersionResponse)
            .thenThrow(new RuntimeException("Service outage"))
            .thenReturn(schemaVersionResponse);

        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade(mockSchemaRegistryClient);

        LoadingCache<UUID, Schema> cache = glueSchemaRegistryDeserializationFacade.cache;

        //Make sure cache is empty to start with.
        assertEquals(0, cache.size());

        assertDoesNotThrow(
            () -> glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData)));

        //Ensure cache only one value as desired.
        assertEquals(1, cache.size());

        Map.Entry<UUID, Schema> cacheEntry = (Map.Entry<UUID, Schema>) cache.asMap().entrySet().toArray()[0];
        Schema expectedSchema = new Schema(inputSchemaDefinition, dataFormat, "test_schema");

        //Verify cache contents.
        assertEquals(schemaVersionId, cacheEntry.getKey());
        assertEquals(expectedSchema, cacheEntry.getValue());

        //Expire cache.
        cache.refresh(schemaVersionId);

        //Failed service call shouldn't result in exceptions.
        assertDoesNotThrow(
            () -> glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData)));
        assertEquals(1, cache.size());

        //Subsequent calls shouldn't fail either.
        assertDoesNotThrow(
                () -> glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData)));
        assertDoesNotThrow(
            () ->glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData)));

        verify(mockSchemaRegistryClient, times(2)).getSchemaVersionResponse(Mockito.eq(schemaVersionId.toString()));

        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE);
        configs.remove(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE);
    }

    /**
     * Tests invoking close method.
     */
    @Test
    public void testClose_succeeds() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                createGSRDeserializationFacade();
        assertDoesNotThrow(() -> glueSchemaRegistryDeserializationFacade.close());
    }

    @ParameterizedTest
    @EnumSource(value = DataFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN_TO_SDK_VERSION", "JSON", "PROTOBUF"})
    public void testCanDeserialize_WhenValidBytesArePassed_ReturnsTrue(DataFormat dataFormat) {
        byte[] validSchemaRegistryBytes = createSerializedCompressedEmployeeData(genericEmployeeAvroRecord, dataFormat);
        assertTrue(createGSRDeserializationFacade().canDeserialize(validSchemaRegistryBytes));
    }

    @Test
    public void testCanDeserialize_WhenNullBytesArePassed_ReturnsFalse() {
        assertFalse(createGSRDeserializationFacade().canDeserialize(null));
    }

    @Test
    public void testCanDeserialize_WhenInvalidBytesArePassed_ReturnsFalse() {
        assertFalse(createGSRDeserializationFacade().canDeserialize(new byte[]{9, 2, 1}));
    }

    /**
     * Helper method to serialize data for testing de-serialization.
     */
    private byte[] createSerializedData(Object objectToSerialize,
                                        DataFormat dataFormat,
                                        String schemaDefinition,
                                        UUID schemaVersionId) {
        GlueSchemaRegistrySerializationFacade serializationFacade = GlueSchemaRegistrySerializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(configs)
                .schemaByDefinitionFetcher(this.mockSchemaByDefinitionFetcher)
                .build();
        when(mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(Mockito.eq(schemaDefinition),
                                                                    Mockito.eq(TEST_SCHEMA_NAME),
                                                                    Mockito.eq(dataFormat.name()),
                                                                    Mockito.anyMap())).thenReturn(schemaVersionId);
        return serializationFacade.serialize(dataFormat, objectToSerialize, schemaVersionId);
    }

    /**
     * Helper method to serialize USER data for testing de-serialization.
     */
    private byte[] createSerializedUserData(Object objectToSerialize,
                                            DataFormat dataFormat) {
        UUID schemaVersionId = glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareSerializerInput(userSchemaDefinition, USER_SCHEMA_NAME, dataFormat.name()));
        return glueSchemaRegistrySerializationFacade.serialize(dataFormat, objectToSerialize, schemaVersionId);
    }

    /**
     * Helper method to serialize EMPLOYEE data for testing de-serialization.
     */
    private byte[] createSerializedEmployeeData(Object objectToSerialize,
                                                DataFormat dataFormat) {
        UUID schemaVersionId = glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareSerializerInput(employeeSchemaDefinition, EMPLOYEE_SCHEMA_NAME, dataFormat.name()));
        return glueSchemaRegistrySerializationFacade.serialize(dataFormat, objectToSerialize, schemaVersionId);
    }

    /**
     * Helper method to serialize EMPLOYEE data for testing de-serialization.
     */
    private byte[] createSerializedCompressedEmployeeData(Object objectToSerialize,
                                                          DataFormat dataFormat) {
        UUID schemaVersionId = compressingGlueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareSerializerInput(employeeSchemaDefinition, EMPLOYEE_SCHEMA_NAME, dataFormat.name()));
        return compressingGlueSchemaRegistrySerializationFacade.serialize(dataFormat, objectToSerialize,
                                                                          schemaVersionId);
    }

    /**
     * Helper method to create GlueSchemaRegistryDeserializationFacade instance.
     *
     * @return GlueSchemaRegistryDeserializationFacade instance.
     */
    private GlueSchemaRegistryDeserializationFacade createGSRDeserializationFacade() {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .credentialProvider(this.mockDefaultCredProvider)
                        .configs(this.configs)
                        .schemaRegistryClient(this.mockDefaultRegistryClient)
                        .build();

        return glueSchemaRegistryDeserializationFacade;
    }

    /**
     * Helper method to create GlueSchemaRegistryDeserializationFacade instance.
     *
     * @param mockClient schema registry mock client
     * @return GlueSchemaRegistryDeserializationFacade instance.
     */
    private GlueSchemaRegistryDeserializationFacade createGSRDeserializationFacade(AWSSchemaRegistryClient mockClient) {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .credentialProvider(this.mockDefaultCredProvider)
                        .configs(this.configs)
                        .schemaRegistryClient(mockClient)
                        .build();

        return glueSchemaRegistryDeserializationFacade;
    }

    /**
     * Helper method to create GlueSchemaRegistryDeserializationFacade instance.
     *
     * @param glueSchemaRegistryDeserializerFactory de-serializer factory instance
     * @return GlueSchemaRegistryDeserializationFacade instance.
     */
    private GlueSchemaRegistryDeserializationFacade createGSRDeserializationFacade(GlueSchemaRegistryDeserializerFactory glueSchemaRegistryDeserializerFactory) {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .credentialProvider(this.mockDefaultCredProvider)
                        .configs(this.configs)
                        .schemaRegistryClient(mockSchemaRegistryClient)
                        .build();

        glueSchemaRegistryDeserializationFacade.setDeserializerFactory(glueSchemaRegistryDeserializerFactory);

        return glueSchemaRegistryDeserializationFacade;
    }

    private AWSSerializerInput prepareSerializerInput(String schemaDefinition,
                                                      String schemaName,
                                                      String dataFormat) {
        return AWSSerializerInput.builder()
                .schemaDefinition(schemaDefinition)
                .schemaName(schemaName)
                .dataFormat(dataFormat)
                .build();
    }

    private AWSDeserializerInput prepareDeserializerInput(byte[] data) {
        return AWSDeserializerInput.builder()
                .buffer(ByteBuffer.wrap(data))
                .build();
    }
}
