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

import com.amazonaws.services.schemaregistry.caching.AWSSchemaRegistryDeserializerCache;
import com.amazonaws.services.schemaregistry.common.AWSDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSIncompatibleDataException;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import com.amazonaws.services.schemaregistry.utils.SchemaLoader;
import com.amazonaws.services.schemaregistry.utils.SerializedByteArrayGenerator;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing protocol agnostic de-serializer.
 */

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AWSDeserializerTest {
    @Mock
    private AWSSchemaRegistryClient mockDefaultRegistryClient;
    @Mock
    private AWSSchemaRegistryClient mockClientThatThrowsException;
    @Mock
    private AWSSchemaRegistryClient mockSchemaRegistryClient;
    @Mock
    private AwsCredentialsProvider mockDefaultCredProvider;
    @Mock
    private AWSDataFormatDeserializer mockDataFormatDeserializer;
    @Mock
    private AWSDeserializerFactory mockDeserializerFactory;

    private final Map<String, Object> configs = new HashMap<>();
    public static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";
    public static final String AVRO_EMP_RECORD_SCHEMA_FILE_PATH = "src/test/java/resources/avro/emp_record.avsc";

    private static final String USER_SCHEMA_NAME = "User";
    private static final UUID USER_SCHEMA_VERSION_ID = UUID.randomUUID();
    private static final String USER_SCHEMA_ARN = "arn:aws:glue:ca-central-1:111111111111:schema/registry_name"
                                                  + "/user_schema";
    private static GenericRecord genericUserAvroRecord;
    private static org.apache.avro.Schema userAvroSchema;
    private static String userSchemaDefinition;
    private static GetSchemaVersionResponse userSchemaVersionResponse;

    private static final String EMPLOYEE_SCHEMA_NAME = "Employee";
    private static final UUID EMPLOYEE_SCHEMA_VERSION_ID = UUID.randomUUID();
    private static final String EMPLOYEE_SCHEMA_ARN = "arn:aws:glue:ca-central-1:111111111111:schema/registry_name"
                                                  + "/user_schema";
    private static GenericRecord genericEmployeeAvroRecord;
    private static org.apache.avro.Schema employeeAvroSchema;
    private static String employeeSchemaDefinition;
    private static GetSchemaVersionResponse employeeSchemaVersionResponse;

    private static AWSAvroSerializer awsAvroSerializer;
    private static AWSAvroSerializer compressingAwsAvroSerializer;

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    public void setup() {
        this.configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        this.configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        when(mockClientThatThrowsException.getSchemaVersionResponse(Mockito.anyString()))
                .thenThrow(new AWSSchemaRegistryException("some runtime exception"));

        awsAvroSerializer = AWSAvroSerializer.builder().credentialProvider(this.mockDefaultCredProvider)
                .configs(configs).schemaRegistryClient(this.mockDefaultRegistryClient).build();

       Map<String, Object> compressionConfig = new HashMap<>();
       compressionConfig.putAll(this.configs);
       compressionConfig.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, AWSSchemaRegistryConstants.COMPRESSION.ZLIB.toString());

       compressingAwsAvroSerializer = AWSAvroSerializer.builder().credentialProvider(this.mockDefaultCredProvider)
                .configs(compressionConfig).schemaRegistryClient(this.mockDefaultRegistryClient).build();

        genericUserAvroRecord = RecordGenerator.createGenericAvroRecord();
        userAvroSchema = SchemaLoader.loadSchema(AVRO_USER_SCHEMA_FILE);
        userSchemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericUserAvroRecord);
        userSchemaVersionResponse = GetSchemaVersionResponse
                .builder()
                .schemaDefinition(userAvroSchema.toString())
                .dataFormat(DataFormat.AVRO)
                .schemaArn(USER_SCHEMA_ARN)
                .build();

        genericEmployeeAvroRecord = RecordGenerator.createGenericEmpRecord();
        employeeAvroSchema = SchemaLoader.loadSchema(AVRO_EMP_RECORD_SCHEMA_FILE_PATH);
        employeeSchemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericEmployeeAvroRecord);
        employeeSchemaVersionResponse = GetSchemaVersionResponse
                .builder()
                .schemaDefinition(employeeAvroSchema.toString())
                .dataFormat(DataFormat.AVRO)
                .schemaArn(EMPLOYEE_SCHEMA_ARN)
                .build();

        when(mockSchemaRegistryClient.getSchemaVersionResponse(Mockito.eq(USER_SCHEMA_VERSION_ID.toString()))).thenReturn(userSchemaVersionResponse);
        when(mockSchemaRegistryClient.getSchemaVersionResponse(Mockito.eq(EMPLOYEE_SCHEMA_VERSION_ID.toString()))).thenReturn(employeeSchemaVersionResponse);

        when(mockDataFormatDeserializer.deserialize(Mockito.eq(EMPLOYEE_SCHEMA_VERSION_ID),
                Mockito.any(ByteBuffer.class), Mockito.eq(employeeAvroSchema.toString()))).thenReturn(genericEmployeeAvroRecord);
        when(mockDataFormatDeserializer.deserialize(Mockito.eq(USER_SCHEMA_VERSION_ID),
                Mockito.any(ByteBuffer.class), Mockito.eq(userAvroSchema.toString()))).thenReturn(genericUserAvroRecord);

        when(mockDeserializerFactory.getInstance(Mockito.any(DataFormat.class),
                Mockito.any(GlueSchemaRegistryConfiguration.class))).thenReturn(mockDataFormatDeserializer);

        when(mockDefaultRegistryClient.getORRegisterSchemaVersionId(Mockito.eq(userSchemaDefinition),
                Mockito.eq(USER_SCHEMA_NAME), Mockito.eq(DataFormat.AVRO.name()), Mockito.anyMap())).thenReturn(USER_SCHEMA_VERSION_ID);
        when(mockDefaultRegistryClient.getORRegisterSchemaVersionId(Mockito.eq(employeeSchemaDefinition),
                Mockito.eq(EMPLOYEE_SCHEMA_NAME), Mockito.eq(DataFormat.AVRO.name()), Mockito.anyMap())).thenReturn(EMPLOYEE_SCHEMA_VERSION_ID);
    }

    /**
     * Tests the AWSDeserializer instantiation when an no configuration is provided.
     */
    @Test
    public void testBuildDeserializer_withNoArguments_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> AWSDeserializer.builder().build());
    }

    /**
     * Tests the AWSDeserializer instantiation when an no configuration is provided.
     */
    @Test
    public void testBuildDeserializer_withNullConfig_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new AWSDeserializer(null, DefaultCredentialsProvider.create()));
    }

    /**
     * Tests the AWSDeserializer and assert for dependency values with config map.
     */
    @Test
    public void testBuildDeserializer_withConfigs_buildsSuccessfully() {
        AWSDeserializer awsDeserializer = AWSDeserializer.builder()
                .credentialProvider(this.mockDefaultCredProvider).schemaRegistryClient(this.mockDefaultRegistryClient)
                .configs(this.configs)
                .build();

        assertEquals(this.mockDefaultCredProvider, awsDeserializer.getCredentialsProvider());
        assertEquals(new GlueSchemaRegistryConfiguration(this.configs), awsDeserializer.getGlueSchemaRegistryConfiguration());
    }

    /**
     * Tests the AWSDeserializer and assert for dependency values with properties.
     */
    @Test
    public void testBuildDeserializer_withProperties_buildsSuccessfully() {
        Properties props = new Properties();
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");

        AWSDeserializer awsDeserializer = AWSDeserializer.builder()
                .credentialProvider(this.mockDefaultCredProvider).schemaRegistryClient(this.mockDefaultRegistryClient)
                .properties(props)
                .build();

        assertEquals(this.mockDefaultCredProvider, awsDeserializer.getCredentialsProvider());
        assertEquals(this.mockDefaultRegistryClient, awsDeserializer.getSchemaRegistryClient());
        assertEquals(new GlueSchemaRegistryConfiguration(props), awsDeserializer.getGlueSchemaRegistryConfiguration());
    }

    /**
     * Tests the AWSDeserializer instantiation when an empty configuration is provided.
     */
    @Test
    public void testBuildDeserializer_emptyConfig_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider).configs(new HashMap<>())
                        .build());
        assertThrows(IllegalArgumentException.class,
                () -> AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider).properties(new Properties())
                        .build());
        assertThrows(IllegalArgumentException.class,
                () -> AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider).configs(new HashMap<>())
                        .schemaRegistryClient(this.mockDefaultRegistryClient).build());
    }

    /**
     * Tests the AWSDeserializer instantiation when an invalid configuration is provided.
     */
    @Test
    public void testBuildDeserializer_badConfig_throwsException() {
        Map<String, ?> badConfig = new HashMap<String, String>() {{
            put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.UNKNOWN_TO_SDK_VERSION.toString());
        }};
        assertThrows(AWSSchemaRegistryException.class,
                () -> AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider).configs(badConfig).build());
        assertThrows(AWSSchemaRegistryException.class,
                () -> AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider)
                        .configs(badConfig).schemaRegistryClient(this.mockDefaultRegistryClient).build());
    }

    /**
     * Tests the AWSDeserializer instantiation when an null configuration is provided.
     */
    @Test
    public void testBuildDeserializer_nullConfig_throwsException() {
        assertThrows(AWSSchemaRegistryException.class,
                () -> AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider).configs(null).build());
        Exception exception = assertThrows(AWSSchemaRegistryException.class,
                () -> AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider)
                        .configs(null).schemaRegistryClient(this.mockDefaultRegistryClient).build());

        assertEquals("Either properties or configuration has to be provided", exception.getMessage());
    }

    /**
     * Tests the AWSDeserializer by retrieving schema definition - positive case.
     */
    @Test
    public void testGetSchemaDefinition_getSchemaVersionFromClientSucceeds_schemaDefinitionMatches() {
        byte[] serializedData = createSerializedUserData(genericUserAvroRecord);

        AWSDeserializer awsDeserializer = createAwsDeserializer(mockSchemaRegistryClient);
        String schemaDefinition = awsDeserializer.getSchemaDefinition(serializedData);
        assertEquals(userAvroSchema.toString(), schemaDefinition);
    }

    /**
     * Tests the AWSDeserializer by retrieving schema definition - negative case.
     */
    @Test
    public void testGetSchemaDefinition_getSchemaVersionFromClientFails_throwsException() {
        GenericRecord genericRecord = RecordGenerator.createGenericAvroRecord();

        AWSSchemaRegistryDeserializerCache deserializerCache = invalidateAndGetCache();

        AWSDeserializer awsDeserializer = createAwsDeserializer(deserializerCache, mockClientThatThrowsException);
        byte[] serializedData = createSerializedUserData(genericRecord);
        assertThrows(AWSSchemaRegistryException.class, () -> awsDeserializer.getSchemaDefinition(serializedData));
    }

    /**
     * Tests the getSchemaVersionId for exception case where data length is invalid.
     */
    @Test
    public void testGetSchemaDefinition_invalidDataLength_throwsException() {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        byte[] serializedData = new byte[]{AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE};
        assertThrows(AWSIncompatibleDataException.class, () -> awsDeserializer.getSchemaDefinition(serializedData));
    }

    /**
     * Tests the getSchemaVersionId for exception case where the header version byte is unknown.
     */
    @Test
    public void testGetSchemaDefinition_invalidHeaderVersionByte_throwsException() {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        byte[] serializedData = SerializedByteArrayGenerator.constructBasicSerializedData((byte) 99, AWSSchemaRegistryConstants.COMPRESSION_BYTE,
                UUID.randomUUID());
        assertThrows(AWSIncompatibleDataException.class, () -> awsDeserializer.getSchemaDefinition(serializedData));
    }

    /**
     * Tests the getSchemaVersionId for exception case where the compression byte is unknown.
     */
    @Test
    public void testGetSchemaDefinition_invalidCompressionByte_throwsException() {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        byte[] serializedData = SerializedByteArrayGenerator.constructBasicSerializedData(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                (byte) 99, UUID.randomUUID());

        assertThrows(AWSIncompatibleDataException.class, () -> awsDeserializer.getSchemaDefinition(serializedData));
    }

    /**
     * Tests the getSchemaVersionId for exception case where the buffer is null.
     */
    @Test
    public void testGetSchemaDefinition_nullBuffer_throwsException() {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        assertThrows(IllegalArgumentException.class, () -> awsDeserializer.getSchemaDefinition((ByteBuffer) null));
    }

    /**
     * Tests the getSchemaVersionId for exception case where the byte array is null.
     */
    @Test
    public void testGetSchemaDefinition_nullByte_throwsException() {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        assertThrows(IllegalArgumentException.class, () -> awsDeserializer.getSchemaDefinition((byte[]) null));
    }

    /**
     * Tests the de-serialization positive case.
     */
    @Test
    public void testDeserialize_withValidAVROSchemaResponse_recordMatches() {
        byte[] serializedUserData = createSerializedUserData(genericUserAvroRecord);
        AWSDeserializer awsDeserializer = createAwsDeserializer(mockDeserializerFactory);

        Object deserializedUserObject = awsDeserializer.deserialize(prepareDeserializerInput(serializedUserData));
        assertEquals(genericUserAvroRecord, deserializedUserObject);
    }

    /**
     * Tests the de-serialization of multiple records of different schemas.
     */
    @Test
    public void testDeserialize_withMultipleRecords_recordsMatch() {
        byte[] serializedUserData = createSerializedUserData(genericUserAvroRecord);
        byte[] serializedEmployeeData = createSerializedEmployeeData(genericEmployeeAvroRecord);

        AWSDeserializer awsDeserializer = createAwsDeserializer(mockDeserializerFactory);
        Object deserializedUserObject = awsDeserializer.deserialize(prepareDeserializerInput(serializedUserData));
        Object deserializedEmployeeObject = awsDeserializer.deserialize(prepareDeserializerInput(serializedEmployeeData));

        assertEquals(deserializedUserObject, deserializedUserObject);
        assertEquals(genericEmployeeAvroRecord, deserializedEmployeeObject);
    }

    /**
     * Tests the de-serialization negative case UnknownDataException.
     */
    @Test
    public void testDeserialize_invalidData_throwsException() {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        byte[] serializedData = new byte[]{AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE};
        assertThrows(AWSIncompatibleDataException.class, () -> awsDeserializer.deserialize(prepareDeserializerInput(serializedData)));
    }

    @Test
    public void testGetActualData_withValidBytes_ReturnsActualBytes() throws IOException {
        byte[] serializedUserData = createSerializedUserData(genericUserAvroRecord);
        AWSDeserializer awsDeserializer = createAwsDeserializer(mockDeserializerFactory);

        ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
        GenericDatumWriter writer = new GenericDatumWriter<GenericRecord>(userAvroSchema);
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(expectedBytes, null);
        writer.write(genericUserAvroRecord, encoder);
        encoder.flush();

        byte[] actualBytes = awsDeserializer.getActualData(serializedUserData);

        //Convert to ByteBuffer and compare.
        assertEquals(ByteBuffer.wrap(expectedBytes.toByteArray()), ByteBuffer.wrap(actualBytes));
    }

    @Test
    public void testGetActualData_withValidCompressedBytes_ReturnsActualBytes() throws IOException {
        byte[] serializedCompressedEmployeeData =
                createSerializedCompressedEmployeeData(genericEmployeeAvroRecord);
        AWSDeserializer awsDeserializer = createAwsDeserializer(mockDeserializerFactory);

        ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
        GenericDatumWriter writer = new GenericDatumWriter<GenericRecord>(employeeAvroSchema);
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(expectedBytes, null);
        writer.write(genericEmployeeAvroRecord, encoder);
        encoder.flush();

        byte[] actualBytes = awsDeserializer.getActualData(serializedCompressedEmployeeData);

        //Convert to ByteBuffer and compare.
        assertEquals(ByteBuffer.wrap(expectedBytes.toByteArray()), ByteBuffer.wrap(actualBytes));
    }

    /**
     * Tests the de-serialization for exception case where the deserializer input is null.
     */
    @Test
    public void testDeserialize_nullDeserializerInput_throwsException() {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        assertThrows(IllegalArgumentException.class, () -> awsDeserializer.deserialize(null));
    }

    /**
     * Tests the deserialization case where retrieved schema data is stored in cache
     */
    @Test
    public void testDeserializer_retrieveSchemaRegistryMetadata_MetadataIsCached() {
        byte[] serializedUserData = createSerializedUserData(genericUserAvroRecord);
        AWSDeserializer awsDeserializer = createAwsDeserializer(mockDeserializerFactory);
        AWSSchemaRegistryDeserializerCache deserializerCache = invalidateAndGetCache();

        awsDeserializer.setCache(deserializerCache);
        assertNull(deserializerCache.get(USER_SCHEMA_VERSION_ID));

        awsDeserializer.deserialize(prepareDeserializerInput(serializedUserData));
        assertNotNull(deserializerCache.get(USER_SCHEMA_VERSION_ID));

        when(mockSchemaRegistryClient.getSchemaVersionResponse(Mockito.eq(USER_SCHEMA_VERSION_ID.toString()))).thenReturn(null);
        assertDoesNotThrow(() -> awsDeserializer.deserialize(prepareDeserializerInput(serializedUserData)));
    }

    /**
     * Tests getDataFormat method for null result by passing null schema version ID
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @Test
    public void testGetDataFormat_nullSchemaVersionId_returnsNull() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        Method method = AWSDeserializer.class.getDeclaredMethod("getDataFormat", UUID.class);
        method.setAccessible(true);

        assertNull(method.invoke(awsDeserializer, (UUID) null));
    }

    /***
     * Tests getDataFormat for AVRO type by passing AVRO schema version ID
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @Test
    public void testGetDataFormat_validSchemaVersionId_returnsDataFormat() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        AWSDeserializer awsDeserializer = createAwsDeserializer(mockDeserializerFactory);
        AWSSchemaRegistryDeserializerCache deserializerCache = invalidateAndGetCache();
        deserializerCache.put(USER_SCHEMA_VERSION_ID, new Schema(userSchemaDefinition, DataFormat.AVRO.name(),
                USER_SCHEMA_NAME));

        awsDeserializer.setCache(deserializerCache);
        Method method = AWSDeserializer.class.getDeclaredMethod("getDataFormat", UUID.class);
        method.setAccessible(true);

        assertEquals(DataFormat.AVRO, method.invoke(awsDeserializer, USER_SCHEMA_VERSION_ID));
    }

    /**
     * Tests invoking close method.
     */
    @Test
    public void testClose_succeeds() {
        AWSDeserializer awsDeserializer = createAwsDeserializer();
        assertDoesNotThrow(() -> awsDeserializer.close());
    }

    @Test
    public void testCanDeserialize_WhenValidBytesArePassed_ReturnsTrue() {
        byte [] validSchemaRegistryBytes = createSerializedCompressedEmployeeData(genericEmployeeAvroRecord);
        assertTrue(createAwsDeserializer().canDeserialize(validSchemaRegistryBytes));
    }

    @Test
    public void testCanDeserialize_WhenNullBytesArePassed_ReturnsFalse() {
        assertFalse(createAwsDeserializer().canDeserialize(null));
    }

    @Test
    public void testCanDeserialize_WhenInvalidBytesArePassed_ReturnsFalse() {
        assertFalse(createAwsDeserializer().canDeserialize(new byte[] {9, 2, 1}));
    }

    /**
     * Helper method to construct and return AWSSchemaRegistryDeserializerCache instance.
     *
     * @return AWSSchemaRegistryDeserializerCache instance with fresh cache
     */
    private AWSSchemaRegistryDeserializerCache invalidateAndGetCache() {
        GlueSchemaRegistryConfiguration mockConfig = mock(GlueSchemaRegistryConfiguration.class);
        AWSSchemaRegistryDeserializerCache deserializerCache =
                AWSSchemaRegistryDeserializerCache.getInstance(mockConfig);
        deserializerCache.flushCache();
        return deserializerCache;
    }

    /**
     * Helper method to serialize USER data for testing de-serialization.
     */
    private byte[] createSerializedUserData(Object objectToSerialize) {
        UUID schemaVersionId = awsAvroSerializer.registerSchema(prepareSerializerInput(userSchemaDefinition, USER_SCHEMA_NAME));
        return awsAvroSerializer.serialize(objectToSerialize, schemaVersionId);
    }

    /**
     * Helper method to serialize EMPLOYEE data for testing de-serialization.
     */
    private byte[] createSerializedEmployeeData(Object objectToSerialize) {
        UUID schemaVersionId = awsAvroSerializer.registerSchema(prepareSerializerInput(employeeSchemaDefinition, EMPLOYEE_SCHEMA_NAME));
        return awsAvroSerializer.serialize(objectToSerialize, schemaVersionId);
    }

    /**
     * Helper method to serialize EMPLOYEE data for testing de-serialization.
     */
    private byte[] createSerializedCompressedEmployeeData(Object objectToSerialize) {
        UUID schemaVersionId =
                compressingAwsAvroSerializer.registerSchema(prepareSerializerInput(employeeSchemaDefinition, EMPLOYEE_SCHEMA_NAME));
        return compressingAwsAvroSerializer.serialize(objectToSerialize, schemaVersionId);
    }

    /**
     * Helper method to create AWSDeserializer instance.
     *
     * @return AWSDeserializer instance.
     */
    private AWSDeserializer createAwsDeserializer() {
        AWSDeserializer awsDeserializer = AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider)
                .configs(this.configs).schemaRegistryClient(this.mockDefaultRegistryClient).build();

        return awsDeserializer;
    }

    /**
     * Helper method to create AWSDeserializer instance.
     *
     * @param cache      de-serializer cache
     * @param mockClient schema registry mock client
     * @return AWSDeserializer instance.
     */
    private AWSDeserializer createAwsDeserializer(AWSSchemaRegistryDeserializerCache cache,
                                                  AWSSchemaRegistryClient mockClient) {
        AWSDeserializer awsDeserializer = AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider)
                .configs(this.configs).schemaRegistryClient(mockClient).build();

        awsDeserializer.setCache(cache);

        return awsDeserializer;
    }

    /**
     * Helper method to create AWSDeserializer instance.
     *
     * @param mockClient schema registry mock client
     * @return AWSDeserializer instance.
     */
    private AWSDeserializer createAwsDeserializer(AWSSchemaRegistryClient mockClient) {
        AWSDeserializer awsDeserializer = AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider)
                .configs(this.configs).schemaRegistryClient(mockClient).build();

        awsDeserializer.setCache(invalidateAndGetCache());

        return awsDeserializer;
    }

    /**
     * Helper method to create AWSDeserializer instance.
     *
     * @param awsDeserializerFactory de-serializer factory instance
     * @return AWSDeserializer instance.
     */
    private AWSDeserializer createAwsDeserializer(AWSDeserializerFactory awsDeserializerFactory) {
        AWSDeserializer awsDeserializer = AWSDeserializer.builder().credentialProvider(this.mockDefaultCredProvider)
                .configs(this.configs).schemaRegistryClient(mockSchemaRegistryClient).build();

        awsDeserializer.setDeserializerFactory(awsDeserializerFactory);
        awsDeserializer.setCache(invalidateAndGetCache());

        return awsDeserializer;
    }

    private AWSSerializerInput prepareSerializerInput(String schemaDefinition, String schemaName) {
        return AWSSerializerInput.builder().schemaDefinition(schemaDefinition)
                .schemaName(schemaName).build();
    }

    private AWSDeserializerInput prepareDeserializerInput(byte[] data) {
        return AWSDeserializerInput.builder().buffer(ByteBuffer.wrap(data)).build();
    }
}
