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

import com.amazonaws.services.schemaregistry.caching.AWSSchemaRegistrySerializerCache;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

public class GlueSchemaRegistrySerializationFacadeTest extends AWSSchemaRegistryValidationUtil {

    public static final String UNKNOWN_DATA_FORMAT = "UNKNOWN_DATA_FORMAT";
    @Mock
    private AWSSchemaRegistryClient mockClient;
    private final Map<String, Object> configs = new HashMap<>();
    public static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";
    public static final String AVRO_USER_ENUM_SCHEMA_FILE = "src/test/java/resources/avro/user_enum.avsc";
    public static final String AVRO_USER_ARRAY_SCHEMA_FILE = "src/test/java/resources/avro/user_array.avsc";
    public static final String AVRO_USER_UNION_SCHEMA_FILE = "src/test/java/resources/avro/user_union.avsc";
    public static final String AVRO_USER_FIXED_SCHEMA_FILE = "src/test/java/resources/avro/user_fixed.avsc";
    public static final String AVRO_USER_ARRAY_LONG_SCHEMA_FILE = "src/test/java/resources/avro/user_array_long.avsc";
    public static final String AVRO_USER_ARRAY_STRING_SCHEMA_FILE = "src/test/java/resources/avro/user_array_String.avsc";
    public static final String AVRO_USER_MAP_SCHEMA_FILE = "src/test/java/resources/avro/user_map.avsc";
    public static final String AVRO_USER_MIXED_TYPE_SCHEMA_FILE = "src/test/java/resources/avro/user3.avsc";
    private static final UUID SCHEMA_VERSION_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final String TRANSPORT_NAME = "default-stream";
    private static final String TEST_SCHEMA = "test-schema";
    private static final String AVRO_SCHEMA_TYPE = DataFormat.AVRO.name();

    private Schema schema = null;
    private User userDefinedPojo;
    private GenericRecord genericRecord;
    private Customer customer;
    @Mock
    private AwsCredentialsProvider cred;

    @BeforeEach
    public void setup() {
        mockClient = mock(AWSSchemaRegistryClient.class);
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

        userDefinedPojo = User.newBuilder().setName("test_avros_schema").setFavoriteColor("violet")
                .setFavoriteNumber(10).build();

        genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", "sansa");
        genericRecord.put("favorite_number", 99);
        genericRecord.put("favorite_color", "red");

        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.METADATA, metadata);
        configs.put(AWSSchemaRegistryConstants.TAGS, testTags);
    }

    /**
     * Helper method to construct and return AWSSchemaRegistrySerializerCache instance.
     *
     * @return AWSSchemaRegistrySerializerCache instance with fresh cache
     */
    private AWSSchemaRegistrySerializerCache createSerializerCache() {
        GlueSchemaRegistryConfiguration mockConfig = mock(GlueSchemaRegistryConfiguration.class);
        AWSSchemaRegistrySerializerCache serializerCache =
                AWSSchemaRegistrySerializerCache.getInstance(mockConfig);
        serializerCache.flushCache();

        return serializerCache;
    }

    private GlueSchemaRegistrySerializationFacade createGlueSerializationFacade(Map<String, Object> configs, AWSSchemaRegistryClient awsSchemaRegistryClient) {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade.builder()
                .glueSchemaRegistryConfiguration(new GlueSchemaRegistryConfiguration(configs))
                .credentialProvider(cred)
                .schemaRegistryClient(awsSchemaRegistryClient)
                .build();

        AWSSchemaRegistrySerializerCache awsSchemaRegistrySerializerCache = createSerializerCache();
        glueSchemaRegistrySerializationFacade.setCache(awsSchemaRegistrySerializerCache);

        return glueSchemaRegistrySerializationFacade;
    }

    /**
     * Tests serialization for generic record.
     */
    @Test
    public void testSerialize_schemaParsing_succeeds() {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade = createGlueSerializationFacade(configs, mockClient);
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord);

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User"), eq(DataFormat.AVRO.name()), anyMap())).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);
        UUID schemaVersionId = glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(prepareInput(schemaDefinition, "User"));

        assertNotNull(glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, genericRecord, schemaVersionId));
    }

    @Test
    public void testSerialize_InvalidDataFormat_ThrowsException() {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade = createGlueSerializationFacade(configs, mockClient);
        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> glueSchemaRegistrySerializationFacade.serialize(
            DataFormat.UNKNOWN_TO_SDK_VERSION, genericRecord, SCHEMA_VERSION_ID_FOR_TESTING));
        assertTrue(exception.getMessage().contains("Unsupported data format:"));
    }

    @Test
    public void testSerialize_NullSchemaVersionId_ThrowsException() {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade = createGlueSerializationFacade(configs, mockClient);
        assertThrows(IllegalArgumentException.class, () -> glueSchemaRegistrySerializationFacade.serialize(
            DataFormat.AVRO, genericRecord, null));
    }

    @Test
    public void testSerialize_NullData_ThrowsException() {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade = createGlueSerializationFacade(configs, mockClient);
        assertThrows(IllegalArgumentException.class, () -> glueSchemaRegistrySerializationFacade.serialize(
            DataFormat.AVRO, null, SCHEMA_VERSION_ID_FOR_TESTING));
    }

    /**
     * Tests serialization for specific record.
     */
    @Test
    public void testSerialize_customPojos_succeeds() {
        GlueSchemaRegistrySerializationFacade glueSerializationFacade = createGlueSerializationFacade(configs, mockClient);
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord);

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User"), eq(AVRO_SCHEMA_TYPE), anyMap())).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);
        UUID schemaVersionId = glueSerializationFacade.getOrRegisterSchemaVersion(prepareInput(schemaDefinition, "User"));

        assertNotNull(glueSerializationFacade.serialize(DataFormat.AVRO, userDefinedPojo, schemaVersionId));
    }

    private AWSSerializerInput prepareInput(String schemaDefinition, String schemaName) {
        return AWSSerializerInput.builder().schemaDefinition(schemaDefinition).schemaName(schemaName).build();
    }

    /**
     * Tests build AWSAvroSerializer without configurations will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testBuildAWSAvroSerializer_nullConfig_throwsException() {
        Assertions.assertThrows(AWSSchemaRegistryException.class, () -> AWSAvroSerializer.builder().configs(null)
                .credentialProvider(cred).schemaRegistryClient(mockClient).build());
    }

    /**
     * Tests build AWSAvroSerializer with null configurations but existing property.
     */
    @Test
    public void testBuildAWSAvroSerializer_nullConfigWithProp_throwsException() {
        Properties properties = new Properties();
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        properties.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");

        Assertions.assertDoesNotThrow(() -> AWSAvroSerializer.builder()
                .configs(null)
                .credentialProvider(cred)
                .properties(properties)
                .build());
    }

    /**
     * Tests build AWSAvroSerializer without configurations will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testBuildAWSAvroSerializer_nullCredentialProvider_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> AWSAvroSerializer.builder()
                .configs(configs)
                .credentialProvider(null)
                .build());
    }

    /**
     * Tests build AWSAvroSerializer with invalid configurations will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testBuildAWSAvroSerializer_invalidConfigs_throwsException() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.UNKNOWN_TO_SDK_VERSION.toString());
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");

        Assertions.assertThrows(AWSSchemaRegistryException.class, () -> AWSAvroSerializer.builder().configs(configs)
                .credentialProvider(cred).schemaRegistryClient(mockClient).build());
    }

    /**
     * Tests build AWSAvroSerializer with compression configuration
     */
    @ParameterizedTest
    @EnumSource(value = AWSSchemaRegistryConstants.COMPRESSION.class, names = { "NONE" }, mode = EnumSource.Mode.EXCLUDE)
    public void testBuildAWSAvroSerializer_withCompression_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockClient);

        assertNotNull(glueSchemaRegistrySerializationFacade.getCache());
    }

    @Test
    public void testInitialize_nullCredentials_ThrowsException() {
        assertThrows(IllegalArgumentException.class,
            () -> new GlueSchemaRegistrySerializationFacade(null, mockClient, new GlueSchemaRegistryConfiguration(configs)));
    }

    /**
     * Tests serialization with null topic.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_nullTopic_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema.Parser parser = new Schema.Parser();
        Schema schemaComprisingEnum = null;

        try {
            schemaComprisingEnum = parser.parse(new File(AVRO_USER_ENUM_SCHEMA_FILE));
        } catch (IOException e) {
            fail("Catch IOException: ", e);
        }

        GenericData.EnumSymbol genericRecord2 = new GenericData.EnumSymbol(schemaComprisingEnum, "ONE");

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord2);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        String nullTopic = null;
        byte[] serializedData = awsKafkaAvroSerializer.serialize(nullTopic, genericRecord2);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization for enum data.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_enums_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema.Parser parser = new Schema.Parser();
        Schema schemaComprisingEnum = null;

        try {
            schemaComprisingEnum = parser.parse(new File(AVRO_USER_ENUM_SCHEMA_FILE));
        } catch (IOException e) {
            fail("Catch IOException: ", e);
        }

        GenericData.EnumSymbol genericRecord2 = new GenericData.EnumSymbol(schemaComprisingEnum, "ONE");

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord2);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", genericRecord2);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization for wrong enum data will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testSerialize_enums_throwsException() {
        Schema schemaComprisingEnum = null;
        Schema.Parser parser = new Schema.Parser();

        try {
            schemaComprisingEnum = parser.parse(new File(AVRO_USER_ENUM_SCHEMA_FILE));
        } catch (IOException e) {
            fail("Catch IOException: ", e);
        }

        GenericData.EnumSymbol genericRecordWrongData = new GenericData.EnumSymbol(schemaComprisingEnum, "SPADE");

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWrongData);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                () -> awsKafkaAvroSerializer.serialize("test-topic", genericRecordWrongData));
    }

    /**
     * Tests serialization for integer array data.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_integerArrays_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Schema schema = getSchema(AVRO_USER_ARRAY_SCHEMA_FILE);
        GenericData.Array<Integer> array = new GenericData.Array<>(1, schema);
        array.add(1);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(array);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", array);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization for object array data.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_objectArrays_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Schema schema = getSchema(AVRO_USER_ARRAY_SCHEMA_FILE);
        GenericData.Array<Object> array = new GenericData.Array<>(1, schema);
        array.add(1);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(array);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", array);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);

    }

    /**
     * Tests serialization for string array data.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_stringArrays_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Schema schema = getSchema(AVRO_USER_ARRAY_STRING_SCHEMA_FILE);
        GenericData.Array<String> array = new GenericData.Array<>(1, schema);
        array.add("2");

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(array);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", array);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization for array data will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testSerialize_arrays_throwsException() throws Exception {
        Schema schema = getSchema(AVRO_USER_ARRAY_SCHEMA_FILE);
        GenericData.Array<Object> array = new GenericData.Array<>(1, schema);
        array.add("s");

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(array);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                () -> awsKafkaAvroSerializer.serialize("test-topic", array));
    }

    /**
     * Tests serialization for map data.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_maps_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Schema schema = getSchema(AVRO_USER_MAP_SCHEMA_FILE);
        GenericData.Record mapRecord = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();

        map.put("test", 1L);
        mapRecord.put("meta", map);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(mapRecord);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", mapRecord);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization for wrong map data will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testSerialize_maps_throwsException() throws Exception {
        Schema schema = getSchema(AVRO_USER_MAP_SCHEMA_FILE);
        GenericData.Record mapRecord = new GenericData.Record(schema);
        Map<String, Object> map = new HashMap<>();

        map.put("test", "s");
        mapRecord.put("meta", map);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(mapRecord);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                () -> awsKafkaAvroSerializer.serialize("test-topic", mapRecord));
    }

    /**
     * Tests serialization for union data.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_unions_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Schema schema = getSchema(AVRO_USER_UNION_SCHEMA_FILE);
        GenericData.Record unionRecord = new GenericData.Record(schema);
        unionRecord.put("experience", 1);
        unionRecord.put("age", 30);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(unionRecord);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", unionRecord);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization for union data with null value.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_unionsNull_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Schema schema = getSchema(AVRO_USER_UNION_SCHEMA_FILE);
        GenericData.Record unionRecord = new GenericData.Record(schema);
        unionRecord.put("experience", null);
        unionRecord.put("age", 30);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(unionRecord);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", unionRecord);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization for fixed data.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_fixed_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Schema schema = getSchema(AVRO_USER_FIXED_SCHEMA_FILE);
        GenericData.Fixed fixedRecord = new GenericData.Fixed(schema);
        byte[] bytes = "byte array".getBytes();
        fixedRecord.bytes(bytes);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(fixedRecord);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", fixedRecord);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization for wrong fixed data will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testSerialize_fixed_throwsException() throws Exception {
        Schema schema = getSchema(AVRO_USER_FIXED_SCHEMA_FILE);
        GenericData.Fixed fixedRecord = new GenericData.Fixed(schema);
        byte[] bytes = "byte".getBytes();
        fixedRecord.bytes(bytes);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(fixedRecord);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                () -> awsKafkaAvroSerializer.serialize("test-topic", fixedRecord));
    }

    /**
     * Tests serialization for wrong union data will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testSerialize_unions_throwsException() throws Exception {
        Schema schema = getSchema(AVRO_USER_UNION_SCHEMA_FILE);
        GenericData.Record unionRecord = new GenericData.Record(schema);
        unionRecord.put("experience", "wrong_value");
        unionRecord.put("age", 30);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(unionRecord);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                () -> awsKafkaAvroSerializer.serialize("test-topic", unionRecord));
    }

    /**
     * Tests serialization for multiple data type.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testSerialize_allTypes_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws Exception {
        Schema schema = getSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE);

        GenericData.EnumSymbol k = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> al = new ArrayList<>();
        al.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);

        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put("meta", map);
        genericRecordWithAllTypes.put("listOfColours", al);
        genericRecordWithAllTypes.put("integerEnum", k);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes);
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);

        byte[] serializedData = awsKafkaAvroSerializer.serialize("test-topic", genericRecordWithAllTypes);
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType);
    }

    /**
     * Tests serialization with unsupported protocol will throw out AWSSchemaRegistryException.
     */
    @Test
    public void testSerialize_unsupportedProtocolMessage_throwsException() throws Exception{
        String fileName = "src/test/java/resources/avro/user_array_long.avsc";
        Schema schema = getSchema(AVRO_USER_ARRAY_LONG_SCHEMA_FILE);

        GenericData.Array<Object> array = new GenericData.Array<>(1, schema);
        array.add(1L);

        ArrayList<Integer> unSupportedFormatArray = new ArrayList<>();
        unSupportedFormatArray.add(1);

        AWSKafkaAvroSerializer awsKafkaAvroSerializer = initialize(configs, null, mockClient,
                null);

        Assertions.assertThrows(AWSSchemaRegistryException.class,
                () -> awsKafkaAvroSerializer.serialize("test-topic", unSupportedFormatArray));
    }

    /**
     * Tests invoking shutdown invokes close method.
     */
    @Test
    public void testClose_succeeds() {
        AWSAvroSerializer avroSerializer = mock(AWSAvroSerializer.class);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = new AWSKafkaAvroSerializer(cred, configs);
        awsKafkaAvroSerializer.setAvroSerializer(avroSerializer);

        awsKafkaAvroSerializer.close();
    }

    /**
     * Tests serialize to check if data reduces after compression.
     */
    @ParameterizedTest
    @EnumSource(value = AWSSchemaRegistryConstants.COMPRESSION.class, names = { "NONE" }, mode = EnumSource.Mode.EXCLUDE)
    public void testSerialize_arraysWithCompression_byteArraySizeIsReduced(AWSSchemaRegistryConstants.COMPRESSION compressionType) throws IOException {
        int capacity = 1000000;
        Schema schema = getSchema(AVRO_USER_ARRAY_STRING_SCHEMA_FILE);
        GenericData.Array<String> array = new GenericData.Array<>(capacity, schema);
        for (int i = 0; i < capacity; i++) { array.add("test"); }

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(array);

        Map<String, Object> configsWithCompressionEnabled = configs.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        configsWithCompressionEnabled.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        AWSKafkaAvroSerializer awsKafkaAvroSerializerWithoutCompression = initialize(configs, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);
        AWSKafkaAvroSerializer awsKafkaAvroSerializerWithCompression = initialize(configsWithCompressionEnabled, schemaDefinition, mockClient,
                SCHEMA_VERSION_ID_FOR_TESTING);
        byte[] serializedData = awsKafkaAvroSerializerWithoutCompression.serialize("test-topic", array);
        byte[] compressedAndSerializedData = awsKafkaAvroSerializerWithCompression.serialize("test-topic", array);

        assertTrue(serializedData.length > compressedAndSerializedData.length);
    }

    /**
     * Tests the serialization case where retrieved schema data is stored in cache
     */
    @Test
    public void testSerializer_retrieveSchemaVersionId_schemaVersionIdIsCached() {
        GlueSchemaRegistrySerializationFacade glueSerializationFacade = createGlueSerializationFacade(configs, mockClient);
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord);

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User"), eq(DataFormat.AVRO.name()), anyMap()))
                .thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);
        com.amazonaws.services.schemaregistry.common.Schema key = new com.amazonaws.services.schemaregistry.common.Schema(schemaDefinition, DataFormat.AVRO.name(), "User");

        AWSSchemaRegistrySerializerCache serializerCache =
                AWSSchemaRegistrySerializerCache.getInstance(new GlueSchemaRegistryConfiguration(configs));
        assertNull(serializerCache.get(key));

        glueSerializationFacade.getOrRegisterSchemaVersion(prepareInput(schemaDefinition, "User"));
        assertNotNull(serializerCache.get(key));

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User"), eq(DataFormat.AVRO.name()), anyMap()))
                .thenReturn(null);
        assertDoesNotThrow(() -> glueSerializationFacade.getOrRegisterSchemaVersion(prepareInput(schemaDefinition, "User")));
    }

    /**
     * Tests registerSchemaVersion method of Serializer with metadata configuration
     */
    @Test
    public void testSerializer_registerSchemaVersion_withMetadataConfig_succeeds() {
        GlueSchemaRegistrySerializationFacade glueSerializationFacade = createGlueSerializationFacade(configs, mockClient);
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord);
        Map<String, String> metadata = getMetadata();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, TRANSPORT_NAME);

        when(mockClient.getORRegisterSchemaVersionId(schemaDefinition, "User", DataFormat.AVRO.name(), metadata))
                .thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);

        UUID schemaVersionId = glueSerializationFacade.getOrRegisterSchemaVersion(prepareInput(schemaDefinition, "User"));
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId);
    }

    /**
     * Tests registerSchemaVersion method of Serializer without metadata configuration
     */
    @Test
    public void testSerializer_registerSchemaVersion_withoutMetadataConfig_succeeds() {
        configs.remove(AWSSchemaRegistryConstants.METADATA);
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade = createGlueSerializationFacade(configs, mockClient);
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord);
        Map<String, String> metadata = new HashMap<>();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, TRANSPORT_NAME);

        when(mockClient.getORRegisterSchemaVersionId(schemaDefinition, "User", DataFormat.AVRO.name(), metadata))
                .thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);

        UUID schemaVersionId = glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(prepareInput(schemaDefinition, "User"));
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId);
    }

    /**
     * Tests registerSchemaVersion method of Serializer when PutSchemaVersionMetadata API throws exception
     */
    @Test
    public void testSerializer_registerSchemaVersion_whenPutSchemaVersionMetadataThrowsException() {
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        AWSSchemaRegistryClient awsSchemaRegistryClient = new AWSSchemaRegistryClient(cred,
            glueSchemaRegistryConfiguration);
        AWSSchemaRegistryClient spyClient = spy(awsSchemaRegistryClient);
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade = createGlueSerializationFacade(configs, spyClient);
        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord);

        Map<String, String> metadata = getMetadata();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, TRANSPORT_NAME);

        EntityNotFoundException entityNotFoundException =
                EntityNotFoundException.builder().message(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG).build();
        AWSSchemaRegistryException awsSchemaRegistryException = new AWSSchemaRegistryException(entityNotFoundException);
        doThrow(awsSchemaRegistryException).when(spyClient)
                .getSchemaVersionIdByDefinition(schemaDefinition, "User", DataFormat.AVRO.name());

        GetSchemaVersionResponse getSchemaVersionResponse = createGetSchemaVersionResponse(SCHEMA_VERSION_ID_FOR_TESTING,
                schemaDefinition,
                DataFormat.AVRO.name());
        doReturn(getSchemaVersionResponse)
                .when(spyClient)
                .registerSchemaVersion(schemaDefinition, "User", DataFormat.AVRO.name());

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            MetadataKeyValuePair metadataKeyValuePair = createMetadataKeyValuePair(entry);
            doThrow(new AWSSchemaRegistryException("Put schema version metadata failed."))
                    .when(spyClient)
                    .putSchemaVersionMetadata(SCHEMA_VERSION_ID_FOR_TESTING, metadataKeyValuePair);
        }
        doNothing()
                .when(spyClient)
                .putSchemaVersionMetadata(SCHEMA_VERSION_ID_FOR_TESTING, metadata);

        UUID schemaVersionId = glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(prepareInput(schemaDefinition, "User"));
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId);
    }

    /**
     * Tests the encode method.
     */
    @Test
    public void testEncode_WhenValidInputIsPassed_EncodesTheBytes() throws Exception {

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecord);

        com.amazonaws.services.schemaregistry.common.Schema schema =
            new com.amazonaws.services.schemaregistry.common.Schema(schemaDefinition, DataFormat.AVRO.name(), TEST_SCHEMA);

        byte[] genericRecordAsBytes = convertGenericRecordToBytes(genericRecord, schema);

        Map<String, String> metadata = getMetadata();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, TRANSPORT_NAME);
        when(mockClient.getORRegisterSchemaVersionId(schemaDefinition, TEST_SCHEMA, AVRO_SCHEMA_TYPE, metadata))
            .thenReturn(SCHEMA_VERSION_ID_FOR_TESTING);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockClient);

        byte[] serializedData = glueSchemaRegistrySerializationFacade
            .encode(TRANSPORT_NAME, schema, genericRecordAsBytes);

        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, AWSSchemaRegistryConstants.COMPRESSION.NONE);
    }

    private byte[] convertGenericRecordToBytes(
        final GenericRecord genericRecord, final com.amazonaws.services.schemaregistry.common.Schema schema)
        throws IOException {
        ByteArrayOutputStream genericRecordAsBytes = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(genericRecordAsBytes, null);
        GenericDatumWriter datumWriter = new GenericDatumWriter<>(AVROUtils.getInstance().getSchema(genericRecord));
        datumWriter.write(genericRecord, encoder);
        encoder.flush();
        return genericRecordAsBytes.toByteArray();
    }

    @Test
    public void testRegisterSchema_nullSerializerInput_throwsException() {
        GlueSchemaRegistrySerializationFacade glueSerializationFacade = createGlueSerializationFacade(configs, mockClient);
        Assertions.assertThrows(IllegalArgumentException.class, () -> glueSerializationFacade.getOrRegisterSchemaVersion(null));
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

    private GetSchemaVersionResponse createGetSchemaVersionResponse(UUID schemaVersionId, String schemaDefinition, String dataFormat) {
        return GetSchemaVersionResponse
                .builder()
                .schemaVersionId(schemaVersionId.toString())
                .schemaDefinition(schemaDefinition)
                .dataFormat(dataFormat)
                .build();
    }

    private MetadataKeyValuePair createMetadataKeyValuePair(Map.Entry<String, String> metadataEntry) {
        return MetadataKeyValuePair
                .builder()
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
