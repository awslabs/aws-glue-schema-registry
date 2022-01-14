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

import com.amazonaws.services.schemaregistry.caching.GlueSchemaRegistryDeserializerCache;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.avro.User;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import com.amazonaws.services.schemaregistry.utils.SchemaLoader;
import com.amazonaws.services.schemaregistry.utils.SerializedByteArrayGenerator;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opentest4j.MultipleFailuresError;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for testing Avro related serialization and de-serialization.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AvroDeserializerTest {
    public static final String AVRO_USER_SCHEMA_FILE = "src/test/resources/avro/user.avsc";
    public static final String AVRO_USER_ENUM_SCHEMA_FILE = "src/test/resources/avro/user_enum.avsc";
    public static final String AVRO_USER_ARRAY_SCHEMA_FILE = "src/test/resources/avro/user_array.avsc";
    public static final String AVRO_USER_UNION_SCHEMA_FILE = "src/test/resources/avro/user_union.avsc";
    public static final String AVRO_USER_FIXED_SCHEMA_FILE = "src/test/resources/avro/user_fixed.avsc";
    public static final String AVRO_USER_ARRAY_STRING_SCHEMA_FILE = "src/test/resources/avro/user_array_String.avsc";
    public static final String AVRO_USER_MAP_SCHEMA_FILE = "src/test/resources/avro/user_map.avsc";
    public static final String AVRO_USER_MIXED_TYPE_SCHEMA_FILE = "src/test/resources/avro/user3.avsc";
    private static final UUID TEST_GENERIC_SCHEMA_VERSION_ID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private final Map<String, Object> configs = new HashMap<>();
    @Mock
    public AwsCredentialsProvider mockDefaultCredProvider;
    @Mock
    private SchemaByDefinitionFetcher mockSchemaByDefinitionFetcher;

    private GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs;

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    public void setup() {
        this.configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        this.configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        this.schemaRegistrySerDeConfigs = new GlueSchemaRegistryConfiguration(this.configs);

        MockitoAnnotations.initMocks(this);
        invalidateAndGetCache();
    }

    /**
     * Helper method to serialize data for testing de-serialization.
     *
     * @param objectToSerialize object for serialization
     * @return serialized ByteBuffer of the object
     */
    public ByteBuffer createBasicSerializedData(Object objectToSerialize,
                                                String compressionType,
                                                DataFormat dataFormat) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType);
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .credentialProvider(this.mockDefaultCredProvider)
                        .configs(configs)
                        .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                        .build();
        return getByteBuffer(objectToSerialize, glueSchemaRegistrySerializationFacade, dataFormat);
    }

    private ByteBuffer getByteBuffer(Object objectToSerialize,
                                     GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade,
                                     DataFormat dataFormat) {
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(dataFormat, objectToSerialize,
                                                                TEST_GENERIC_SCHEMA_VERSION_ID);
        return ByteBuffer.wrap(serializedData);
    }

    /**
     * Creates a AvroSerializer object for testing.
     *
     * @return AvroSerializer object instance
     */
    private GlueSchemaRegistrySerializationFacade createGlueSchemaRegistryFacade(String compressionType) {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType);
        return GlueSchemaRegistrySerializationFacade.builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(configs)
                .schemaByDefinitionFetcher(this.mockSchemaByDefinitionFetcher)
                .build();
    }

    /**
     * Creates and returns a de-serialized object.
     *
     * @param schema         schema for de-serialization
     * @param serializedData serialized data
     * @return de-serialized object
     */
    private Object createDeserializedObjectForGenericRecord(Schema schema, byte[] serializedData) {
        AvroDeserializer avroDeserializer = AvroDeserializer
                .builder()
                .configs(this.schemaRegistrySerDeConfigs)
                .build();
        avroDeserializer.setAvroRecordType(AvroRecordType.GENERIC_RECORD);

        return avroDeserializer.deserialize(ByteBuffer.wrap(serializedData), schema.toString());
    }

    /**
     * De-serializes the given byte array to an Object.
     *
     * @param data   data to de-serialize as byte array
     * @param schema schema for the data
     * @return de-serialized object
     */
    private Object deserialize(GlueSchemaRegistryDataFormatDeserializer deserializer,
                               @NonNull byte[] data,
                               String schema) {
        return deserializer.deserialize(ByteBuffer.wrap(data), schema);
    }

    /**
     * De-serializes the given ByteBuffer to an Object. Overload without accepting schema version id.
     *
     * @param buffer data to de-serialize as ByteBuffer
     * @param schema schema for the data
     * @return de-serialized object
     */
    private Object deserialize(GlueSchemaRegistryDataFormatDeserializer deserializer,
                               @NonNull ByteBuffer buffer,
                               String schema) {
        return deserializer.deserialize(buffer, schema);
    }

    /**
     * De-serializes and asserts the de-serialized object.
     *
     * @param schema           Avro schema object
     * @param serializedObject serialized object for comparison
     * @param serializedData   serialized data bye array
     */
    private void deserializeAndAssertGenericRecord(Schema schema, Object serializedObject, byte[] serializedData) {
        Object deserializedObject = createDeserializedObjectForGenericRecord(schema, serializedData);
        assertTrue(serializedObject.equals(deserializedObject));
    }

    /**
     * Helper method to create AvroDeserializer instance for given record type.
     *
     * @param recordType Generic or Specific record
     * @return AvroDeserializer instance
     */
    public AvroDeserializer createAvroDeserializer(AvroRecordType recordType) {

        AvroDeserializer avroDeserializer = AvroDeserializer
                .builder()
                .configs(this.schemaRegistrySerDeConfigs)
                .build();
        avroDeserializer.setAvroRecordType(recordType);
        return avroDeserializer;
    }

    /**
     * Helper method to construct and return GlueSchemaRegistryDeserializerCache instance.
     *
     * @return GlueSchemaRegistryDeserializerCache instance with fresh cache
     */
    private GlueSchemaRegistryDeserializerCache invalidateAndGetCache() {
        GlueSchemaRegistryConfiguration mockConfig = mock(GlueSchemaRegistryConfiguration.class);
        GlueSchemaRegistryDeserializerCache deserializerCache =
                GlueSchemaRegistryDeserializerCache.getInstance(mockConfig);
        deserializerCache.flushCache();
        return deserializerCache;
    }

    /**
     * Tests creating Avro de-serializer instance and checks for config instance.
     */
    @Test
    public void testCreateAvroDeserializer_withOnlyConfigs_configsMatch() {
        AvroDeserializer avroDeserializer = AvroDeserializer
                .builder()
                .configs(this.schemaRegistrySerDeConfigs)
                .build();

        assertEquals(this.schemaRegistrySerDeConfigs, avroDeserializer.getSchemaRegistrySerDeConfigs());
    }

    /**
     * Tests the de-serialization for exception case where data length is invalid.
     */
    @Test
    public void testDeserialize_incompleteData_throwsException() {
        byte[] serializedData = new byte[]{AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE};

        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD);
        Exception ex = assertThrows(AWSSchemaRegistryException.class, () -> avroDeserializer.deserialize(ByteBuffer.wrap(serializedData), schema.toString()));
        Throwable rootCause = ex.getCause();
        assertTrue(rootCause instanceof GlueSchemaRegistryIncompatibleDataException);
        assertEquals("Data is not compatible with schema registry size: 2", rootCause.getMessage());
    }

    /**
     * Tests the de-serialization for exception case where the header version byte
     * is unknown.
     */
    @Test
    public void testDeserialize_invalidHeaderVersionByte_throwsException() {
        ByteBuffer serializedData = SerializedByteArrayGenerator.constructBasicSerializedByteBuffer((byte) 99,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE, UUID.randomUUID());

        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD);
        Exception ex = assertThrows(AWSSchemaRegistryException.class, () -> avroDeserializer.deserialize(serializedData, schema.toString()));
        Throwable rootCause = ex.getCause();
        assertTrue(rootCause instanceof GlueSchemaRegistryIncompatibleDataException);
        assertEquals("Invalid schema registry header version byte in data", rootCause.getMessage());
    }

    /**
     * Tests the de-serialization for exception case where the compression byte is
     * unknown.
     */
    @Test
    public void testDeserialize_invalidCompressionByte_throwsException() {
        ByteBuffer serializedData =
                SerializedByteArrayGenerator.constructBasicSerializedByteBuffer(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, (byte) 99, UUID.randomUUID());

        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD);
        Exception ex = assertThrows(AWSSchemaRegistryException.class, () -> avroDeserializer.deserialize(serializedData, schema.toString()));
        Throwable rootCause = ex.getCause();
        assertTrue(rootCause instanceof GlueSchemaRegistryIncompatibleDataException);
        assertEquals("Invalid schema registry compression byte in data", rootCause.getMessage());
    }

    /**
     * Test whether the serialized generic record can be de-serialized back to the
     * generic record instance.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_genericRecord_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        GenericRecord genericRecord = RecordGenerator.createGenericAvroRecord();

        ByteBuffer serializedData = createBasicSerializedData(genericRecord, compressionType.name(), DataFormat.AVRO);
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);

        Object deserializedObject = avroDeserializer.deserialize( serializedData,
                                                                 schema.toString());
        assertGenericRecord(genericRecord, deserializedObject);
        //Assert the instance is getting cached.
        assertEquals(1, avroDeserializer.getDatumReaderCache().size());
    }

    public void assertGenericRecord(GenericRecord genericRecord, Object deserializedObject) {
        assertTrue(deserializedObject instanceof GenericRecord);
        assertTrue(deserializedObject.equals(genericRecord));
    }

    /**
     * Test whether the serialized generic record with specific record de-serializer
     * mode can be de-serialized back to a the user defined custom object and the
     * values are same between the generic record and custom object.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_genericRecordWithSpecificMode_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        GenericRecord genericRecord = RecordGenerator.createGenericAvroRecord();

        ByteBuffer serializedData = createBasicSerializedData(genericRecord, compressionType.name(), DataFormat.AVRO);
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD);

        Object deserializedObject = avroDeserializer.deserialize(serializedData,
                                                                 schema.toString());

        //Assert the instance is getting cached.
        assertEquals(1, avroDeserializer.getDatumReaderCache().size());
        assertGenericRecordWithSpecificRecordMode(genericRecord, deserializedObject);
    }

    private void assertGenericRecordWithSpecificRecordMode(GenericRecord genericRecord, Object deserializedObject)
            throws MultipleFailuresError {
        User deserializedUserObject = (User) deserializedObject;
        assertAll("Deserialization is successful!", () -> assertNotNull(deserializedObject),
                () -> assertEquals(genericRecord.get("name"), deserializedUserObject.getName().toString()),
                () -> assertEquals(genericRecord.get("favorite_number"), deserializedUserObject.getFavoriteNumber()),
                () -> assertEquals(genericRecord.get("favorite_color"),
                        deserializedUserObject.getFavoriteColor().toString()));
    }

    /**
     * Test whether the serialized user defined custom object with specific record
     * de-serializer mode can be de-serialized back to a the user defined custom
     * object.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_specificRecord_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        User userDefinedObject = RecordGenerator.createSpecificAvroRecord();
        ByteBuffer serializedData = createBasicSerializedData(userDefinedObject, compressionType.name(), DataFormat.AVRO);

        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD);

        Object deserializedObject = avroDeserializer.deserialize( serializedData, schema.toString());
        assertAll("De-serialized object is User type and equals the serialized object",
                () -> assertTrue(deserializedObject instanceof User),
                () -> assertTrue(deserializedObject.equals(userDefinedObject)));
    }

    /**
     * Test whether the serialized generic record can be de-serialized back to the
     * generic record instance.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_genericRecordWithoutSchemaVersionId_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        GenericRecord genericRecord = RecordGenerator.createGenericAvroRecord();
        ByteBuffer serializedData = createBasicSerializedData(genericRecord, compressionType.name(), DataFormat.AVRO);

        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);
        Object deserializedObject = deserialize(avroDeserializer, serializedData, schema.toString());

        assertGenericRecord(genericRecord, deserializedObject);
    }

    /**
     * Test whether the serialized generic record can be de-serialized back to the
     * generic record instance.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_genericRecordWithByteArray_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        GenericRecord genericRecord = RecordGenerator.createGenericAvroRecord();
        ByteBuffer serializedData = createBasicSerializedData(genericRecord, compressionType.name(), DataFormat.AVRO);

        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);
        Object deserializedObject = deserialize(avroDeserializer, serializedData.array(), schema.toString());

        assertGenericRecord(genericRecord, deserializedObject);
    }

    /**
     * Test whether the serialized user defined custom object with generic record
     * de-serializer mode can be de-serialized back to a generic record object and
     * the values are same between t two custom objects.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_specificRecordInGenericMode_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        User userDefinedObject = RecordGenerator.createSpecificAvroRecord();
        ByteBuffer serializedData = createBasicSerializedData(userDefinedObject, compressionType.name(), DataFormat.AVRO);

        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);
        Object deserializedObject = avroDeserializer.deserialize(serializedData, schema.toString());

        assertSpecificRecordInGenericRecordMode(userDefinedObject, deserializedObject);
    }

    private void assertSpecificRecordInGenericRecordMode(User userDefinedObject, Object deserializedObject)
            throws MultipleFailuresError {
        GenericRecord deserializedGenericRecord = (GenericRecord) deserializedObject;
        assertAll("Deserialization is successful!", () -> assertNotNull(deserializedObject),
                () -> assertEquals(userDefinedObject.getName(), deserializedGenericRecord.get("name").toString()),
                () -> assertEquals(userDefinedObject.getFavoriteNumber(), userDefinedObject.get("favorite_number")),
                () -> assertEquals(userDefinedObject.getFavoriteColor(),
                        userDefinedObject.get("favorite_color").toString()));
    }

    /**
     * Test whether serialized enum can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_enumSchema_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema schemaForEnum = SchemaLoader.loadAvroSchema(AVRO_USER_ENUM_SCHEMA_FILE);
        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(schemaForEnum, "ONE");

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, enumSymbol, UUID.randomUUID());

        deserializeAndAssertGenericRecord(schemaForEnum, enumSymbol, serializedData);
    }

    /**
     * Test whether serialized integer array can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_integerArrays_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema schemaForArray = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_SCHEMA_FILE);
        GenericData.Array<Integer> array = new GenericData.Array<>(1, schemaForArray);
        array.add(1);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, array, UUID.randomUUID());

        deserializeAndAssertGenericRecord(schemaForArray, array, serializedData);
    }

    /**
     * Test whether serialized object array can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_objectArrays_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema schemaForArray = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_SCHEMA_FILE);
        GenericData.Array<Object> array = new GenericData.Array<>(1, schemaForArray);
        array.add(1);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData = glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, array, UUID.randomUUID());

        deserializeAndAssertGenericRecord(schemaForArray, array, serializedData);
    }

    /**
     * Test whether serialized union object can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_unions_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema schemaForUnion = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE);
        GenericData.Record unionRecord = new GenericData.Record(schemaForUnion);
        unionRecord.put("experience", 1);
        unionRecord.put("age", 30);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, unionRecord, UUID.randomUUID());

        deserializeAndAssertGenericRecord(schemaForUnion, unionRecord, serializedData);
    }

    /**
     * Test whether serialized union object with null value can be de-serialized
     * back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_unionsWithNull_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema schemaForUnion = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE);
        GenericData.Record unionRecord = new GenericData.Record(schemaForUnion);
        unionRecord.put("experience", null);
        unionRecord.put("age", 30);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, unionRecord, UUID.randomUUID());
        deserializeAndAssertGenericRecord(schemaForUnion, unionRecord, serializedData);
    }

    /**
     * Test whether serialized fixed array can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_fixedArray_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema schemaForFixedByteArray = SchemaLoader.loadAvroSchema(AVRO_USER_FIXED_SCHEMA_FILE);
        GenericData.Fixed fixedRecord = new GenericData.Fixed(schemaForFixedByteArray);
        byte[] bytes = "byte array".getBytes();
        fixedRecord.bytes(bytes);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, fixedRecord, UUID.randomUUID());

        deserializeAndAssertGenericRecord(schemaForFixedByteArray, fixedRecord, serializedData);
    }

    /**
     * Test whether serialized string array can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_stringArrays_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema schemaForArray = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_STRING_SCHEMA_FILE);
        GenericData.Array<String> array = new GenericData.Array<>(1, schemaForArray);
        array.add("TestValue");

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, array, UUID.randomUUID());

        Object deserializedObject = createDeserializedObjectForGenericRecord(schemaForArray, serializedData);
        validateStringRecords(array, deserializedObject);
    }

    private void validateStringRecords(GenericData.Array<String> array, Object deserializedObject) {
        @SuppressWarnings("unchecked") String actualValue = ((GenericData.Array<Utf8>) deserializedObject)
                .get(0)
                .toString();
        assertEquals(array.get(0), actualValue);
    }

    /**
     * Test whether serialized map can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_maps_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        final String avroRecordMapName = "meta";
        final String keyName = "testKey";
        Schema schemaForMap = SchemaLoader.loadAvroSchema(AVRO_USER_MAP_SCHEMA_FILE);
        GenericData.Record mapRecord = new GenericData.Record(schemaForMap);
        Map<String, Long> map = new HashMap<>();
        map.put(keyName, 1L);
        mapRecord.put(avroRecordMapName, map);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, mapRecord, UUID.randomUUID());

        Object deserializedObject = createDeserializedObjectForGenericRecord(schemaForMap, serializedData);
        validateEnumRecord(avroRecordMapName, keyName, map, deserializedObject);
    }

    private void validateEnumRecord(final String avroRecordMapName, final String keyName, Map<String, Long> map,
                                    Object deserializedObject) {
        @SuppressWarnings("unchecked") Map<Utf8, Long> deserializedMap =
                (HashMap<Utf8, Long>) ((GenericData.Record) deserializedObject).get(avroRecordMapName);
        assertEquals(map
                .keySet()
                .iterator()
                .next(), deserializedMap
                .keySet()
                .iterator()
                .next()
                .toString());
        assertEquals(map.get(keyName), deserializedMap.get(new Utf8(keyName)));
    }

    /**
     * Test for combination of types and check for de-serialized values.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_allTypes_equalsOriginal(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE);
        final String avroRecordMapName = "meta";
        final String keyName = "testKey";

        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> integerArrayList = new ArrayList<>();
        integerArrayList.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put(keyName, 1L);
        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put(avroRecordMapName, map);
        genericRecordWithAllTypes.put("listOfColours", integerArrayList);
        genericRecordWithAllTypes.put("integerEnum", enumSymbol);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                createGlueSchemaRegistryFacade(compressionType.name());
        byte[] serializedData =
                glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, genericRecordWithAllTypes,
                                                                UUID.randomUUID());

        Object deserializedObject = createDeserializedObjectForGenericRecord(schema, serializedData);

        validateRecord(avroRecordMapName, keyName, enumSymbol, integerArrayList, map, deserializedObject);
    }

    private void validateRecord(final String avroRecordMapName, final String keyName,
                                GenericData.EnumSymbol enumSymbol,
                                ArrayList<Integer> integerArrayList,
                                Map<String, Long> map, Object deserializedObject) {
        GenericData.Record deserializedRecord = (GenericData.Record) deserializedObject;
        assertEquals("Joe", deserializedRecord
                .get("name")
                .toString());
        assertEquals(1, deserializedRecord.get("favorite_number"));

        validateEnumRecord(avroRecordMapName, keyName, map, deserializedObject);

        assertEquals(integerArrayList, deserializedRecord.get("listOfColours"));

        assertEquals(enumSymbol, deserializedRecord.get("integerEnum"));
    }

    /**
     * Test invalid record type configuration.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_unknownRecordType_throwsException(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        GenericRecord genericRecord = RecordGenerator.createGenericAvroRecord();
        ByteBuffer serializedData = createBasicSerializedData(genericRecord, compressionType.name(), DataFormat.AVRO);

        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE);
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.UNKNOWN);

        Exception ex = assertThrows(AWSSchemaRegistryException.class,
                     () -> deserialize(avroDeserializer, serializedData.array(), schema.toString()));
        Throwable rootCause = ex.getCause().getCause();
        assertTrue(rootCause instanceof UnsupportedOperationException);
        assertEquals("Unsupported AvroRecordType: UNKNOWN", rootCause.getMessage());
    }

    /**
     * Tests the de-serialization for Schema parse errors by simulating
     * SchemaParseException which will be wrapper under AWSSchemaRegistryException
     * for invalid schemas.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testDeserialize_invalidSchema_throwsException(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        GenericRecord genericRecord = RecordGenerator.createGenericAvroRecord();
        ByteBuffer serializedData = createBasicSerializedData(genericRecord, compressionType.name(), DataFormat.AVRO);

        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);

        assertThrows(AWSSchemaRegistryException.class, () -> avroDeserializer.deserialize(serializedData,
                                                                                          "InvalidSchema"));
    }

    /**
     * Test deserialize for null pointer exception by passing null byte data.
     */
    @Test
    public void testDeserialize_nullData_throwsException() {
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);
        assertThrows((IllegalArgumentException.class), () -> deserialize(avroDeserializer, (byte[]) null,
                                                                         "test-schema-name"));
    }

    /**
     * Test deserialize for null pointer exception by passing null buffer .
     */
    @Test
    public void testDeserialize_nullByteBuffer_throwsException() {
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);
        assertThrows((IllegalArgumentException.class), () -> deserialize(avroDeserializer, (ByteBuffer) null, "test-schema"));
    }

    /**
     * Test deserialize for null pointer exception by passing data and null schema.
     */
    @Test
    public void testDeserialize_nullSchemaWithData_throwsException() {
        byte[] serializedData = getTestSerializedByteData();
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);

        assertThrows((IllegalArgumentException.class), () -> deserialize(avroDeserializer, serializedData, null));
    }

    /**
     * Test deserialize for null pointer exception by passing buffer and null schema.
     */
    @Test
    public void testDeserialize_nullSchemaWithBuffer_throwsException() {
        ByteBuffer serializedByteBuffer = getTestSerializedByteBufferData();
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);

        assertThrows((IllegalArgumentException.class), () -> deserialize(avroDeserializer, serializedByteBuffer, null));
    }

    /**
     * Test deserialize for null pointer exception by passing schemaVersionId, schema and null buffer.
     */
    @Test
    public void testDeserialize_withSchemaVersionIdWithNullBufferWithSchema_throwsException() {
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);
        assertThrows((IllegalArgumentException.class), () -> avroDeserializer.deserialize(null, "test-schema"));
    }

    /**
     * Test deserialize for null pointer exception by passing schemaVersionId, buffer and null schema.
     */
    @Test
    public void testDeserialize_withSchemaVersionIdWithBufferWithNullSchema_throwsException() {
        ByteBuffer serializedByteBuffer = getTestSerializedByteBufferData();
        AvroDeserializer avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD);

        assertThrows((IllegalArgumentException.class), () -> avroDeserializer.deserialize(serializedByteBuffer, null));
    }

    /**
     * Helper method to get SerializedByteBuffer Data for test
     */
    public ByteBuffer getTestSerializedByteBufferData() {
        ByteBuffer serializedByteBuffer = SerializedByteArrayGenerator.constructBasicSerializedByteBuffer((byte) 99,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE, UUID.randomUUID());
        return serializedByteBuffer;
    }

    /**
     * Helper method to get SerializedByte Data for test
     */
    public byte[] getTestSerializedByteData() {
        byte[] serializedByteData = new byte[]{AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE};
        return serializedByteData;
    }
}
