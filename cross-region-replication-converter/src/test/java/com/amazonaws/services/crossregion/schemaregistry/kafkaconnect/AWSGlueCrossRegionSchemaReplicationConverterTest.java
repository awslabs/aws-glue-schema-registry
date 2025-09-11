package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.cache.LoadingCache;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest;
import software.amazon.awssdk.services.glue.model.CreateSchemaResponse;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.ListSchemaVersionsRequest;
import software.amazon.awssdk.services.glue.model.ListSchemaVersionsResponse;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaStatus;
import software.amazon.awssdk.services.glue.model.SchemaVersionListItem;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing RegisterSchema class.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)

public class AWSGlueCrossRegionSchemaReplicationConverterTest {
    @Mock
    private AwsCredentialsProvider credProvider;
    @Mock
    private GlueSchemaRegistryDeserializerImpl deserializer;
    @Mock
    private GlueSchemaRegistrySerializerImpl serializer;
    private GlueClient mockGlueClient;
    private AWSSchemaRegistryClient sourceSchemaRegistryClient;
    private AWSSchemaRegistryClient targetSchemaRegistryClient;
    private final static byte[] ENCODED_DATA = new byte[] { 8, 9, 12, 83, 82 };
    private final static byte[] USER_DATA = new byte[] { 12, 83, 82 };
    private static final String testTopic = "User-Topic";
    private AWSGlueCrossRegionSchemaReplicationConverter converter;
    private static final UUID SCHEMA_ID_FOR_TESTING = UUID.fromString("f8b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final UUID SCHEMA_ID_FOR_TESTING2 = UUID.fromString("310153e9-9a54-4b12-a513-a23fc543ed2f");
    private AWSSchemaRegistryClient awsSchemaRegistryClient;
    private String userSchemaDefinition;
    private String userSchemaDefinition2;

    byte[] genericBytes = new byte[] {3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
            93, -23, -17, 12, 99, 10, 115, 97, 110, 115, 97, -58, 1, 6, 114, 101, 100};
    byte[] avroBytes = new byte[] {3, 0, 84, 24, 47, -109, 37, 124, 74, 77, -100,
            -98, -12, 118, 41, 32, 57, -66, 30, 101, 110, 116, 101, 114, 116, 97, 105, 110, 109, 101, 110,
            116, 95, 50, 0, 0, 0, 0, 0, 0, 20, 64};
    byte[] jsonBytes = new byte[] {3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 123, 34,
            102, 105, 114, 115, 116, 78, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 34, 44, 34, 108, 97,
            115, 116, 78, 97, 109, 101, 34, 58, 34, 68, 111, 101, 34, 44, 34, 97, 103, 101, 34, 58, 50, 49,
            125};
    byte[] protobufBytes = "foo".getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    void setUp() {
        mockGlueClient = mock(GlueClient.class);
        awsSchemaRegistryClient = new AWSSchemaRegistryClient(mockGlueClient);
        sourceSchemaRegistryClient = new AWSSchemaRegistryClient(mockGlueClient);
        targetSchemaRegistryClient = new AWSSchemaRegistryClient(mockGlueClient);
        userSchemaDefinition = "{Some-avro-schema}";
        userSchemaDefinition2 = "{Some-avro-schema-v2}";
    }

    /**
     * Test for Converter config method.
     */
    @Test
    public void testConverter_configure() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);

        assertNotNull(converter);
        assertNotNull(converter.getCredentialsProvider());
        assertNotNull(converter.getSerializer());
        assertNotNull(converter.getDeserializer());
        assertNotNull(converter.isKey());
    }

    /**
     * Test for Converter when source region config is not provided.
     */
    @Test
    public void testConverter_sourceRegionNotProvided_throwsException(){
        converter = new AWSGlueCrossRegionSchemaReplicationConverter();
        Exception exception = assertThrows(DataException.class, () -> converter.configure(getNoSourceRegionProperties(), false));
        assertEquals("Source Region is not provided.", exception.getMessage());
    }

    /**
     * Test for Converter when source registry config is not provided.
     */
    @Test
    public void testConverter_sourceRegistryNotProvided_throwsException(){
        converter = new AWSGlueCrossRegionSchemaReplicationConverter();
        Exception exception = assertThrows(DataException.class, () -> converter.configure(getNoSourceRegistryProperties(), false));
        assertEquals("Source Registry is not provided.", exception.getMessage());
    }

    /**
     * Test for Converter when source endpoint config is not provided.
     */
    @Test
    public void testConverter_sourceEndpointNotProvided_throwsException(){
        converter = new AWSGlueCrossRegionSchemaReplicationConverter();
        Exception exception = assertThrows(DataException.class, () -> converter.configure(getNoSourceEndpointProperties(), false));
        assertEquals("Source Endpoint is not provided.", exception.getMessage());
    }

    /**
     * Test for Converter when target region config is not provided.
     */
    @Test
    public void testConverter_targetRegionNotProvided_throwsException(){
        converter = new AWSGlueCrossRegionSchemaReplicationConverter();
        Exception exception = assertThrows(DataException.class, () -> converter.configure(getNoTargetRegionProperties(), false));
        assertEquals("Target Region is not provided.", exception.getMessage());
    }

    /**
     * Test for Converter when source registry config is not provided.
     */
    @Test
    public void testConverter_targetRegistryNotProvided_throwsException(){
        converter = new AWSGlueCrossRegionSchemaReplicationConverter();
        Exception exception = assertThrows(DataException.class, () -> converter.configure(getNoTargetRegistryProperties(), false));
        assertEquals("Target Registry is not provided.", exception.getMessage());
    }

    /**
     * Test for Converter when source endpoint config is not provided.
     */
    @Test
    public void testConverter_targetEndpointNotProvided_throwsException(){
        converter = new AWSGlueCrossRegionSchemaReplicationConverter();
        Exception exception = assertThrows(DataException.class, () -> converter.configure(getNoTargetEndpointProperties(), false));
        assertEquals("Target Endpoint is not provided.", exception.getMessage());
    }

    /**
     * Test for Converter when no target specific config is provided
     */
    @Test
    public void testConverter_noTargetDetails_Succeeds() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> configs = getPropertiesNoTargetDetails();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);

        assertNotNull(converter.getSerializer());
    }

    /**
     * Test Converter when it returns null given the input value is null.
     */
    @Test
    public void testConverter_fromConnectData_returnsByte0() throws NoSuchFieldException, IllegalAccessException {
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);

        assertNull(converter.fromConnectData(testTopic, expected.schema(), null));
    }

    /**
     * Test Converter when serializer throws exception with Avro schema.
     */
    @Test
    public void testConverter_fromConnectData_serializer_avroSchema_throwsException() throws NoSuchFieldException, IllegalAccessException {
        Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", DataFormat.AVRO.name(), "schemaFoo");
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(SCHEMA_REGISTRY_SCHEMA, SCHEMA_ID_FOR_TESTING);

        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        when(serializer.encode(testTopic, SCHEMA_REGISTRY_SCHEMA, USER_DATA)).thenThrow(new AWSSchemaRegistryException());
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when the deserializer throws exception with Avro schema.
     */
    @Test
    public void testConverter_fromConnectData_deserializer_avroSchema_throwsException() throws NoSuchFieldException, IllegalAccessException {
        Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", DataFormat.AVRO.name(), "schemaFoo");
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(SCHEMA_REGISTRY_SCHEMA, SCHEMA_ID_FOR_TESTING);

        when((deserializer).getData(genericBytes)).thenThrow(new AWSSchemaRegistryException());
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode(null, SCHEMA_REGISTRY_SCHEMA, USER_DATA);
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when Avro schema is replicated.
     */
    @Test
    public void testConverter_fromConnectData_avroSchema_succeeds() throws NoSuchFieldException, IllegalAccessException {
        String schemaDefinition = "{\"namespace\":\"com.amazonaws.services.schemaregistry.serializers.avro\",\"type\":\"record\",\"name\":\"payment\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"id_6\",\"type\":\"double\"}]}";
        Schema testSchema = new Schema(schemaDefinition, DataFormat.AVRO.name(), testTopic);
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(testSchema, SCHEMA_ID_FOR_TESTING);

        doReturn(genericBytes).
                when(deserializer).getData(avroBytes);
        doReturn(testSchema).
                when(deserializer).getSchema(avroBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode(testTopic, testSchema, genericBytes);
        assertEquals(converter.fromConnectData(testTopic, expected.schema(), avroBytes), ENCODED_DATA);
    }

    /**
     * Test Converter when serializer throws exception with JSON schema.
     */
    @Test
    public void testConverter_fromConnectData_serializer_jsonSchema_throwsException() throws NoSuchFieldException, IllegalAccessException {
        Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", DataFormat.JSON.name(), "schemaFoo");
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(SCHEMA_REGISTRY_SCHEMA, SCHEMA_ID_FOR_TESTING);

        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        when(serializer.encode(testTopic, SCHEMA_REGISTRY_SCHEMA, USER_DATA)).thenThrow(new AWSSchemaRegistryException());
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when the deserializer throws exception with JSON schema.
     */
    @Test
    public void testConverter_fromConnectData_deserializer_jsonSchema_throwsException() throws NoSuchFieldException, IllegalAccessException {
        Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", DataFormat.JSON.name(), "schemaFoo");
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(SCHEMA_REGISTRY_SCHEMA, SCHEMA_ID_FOR_TESTING);

        when((deserializer).getData(genericBytes)).thenThrow(new AWSSchemaRegistryException());
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode("schemaFoo", SCHEMA_REGISTRY_SCHEMA, USER_DATA);
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when JSON schema is replicated.
     */
    @Test
    public void testConverter_fromConnectData_jsonSchema_succeeds() throws NoSuchFieldException, IllegalAccessException {
        String testSchemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                + "\"maximum\":180}},\"additionalProperties\":false}";
        Schema testSchema = new Schema(testSchemaDefinition, DataFormat.JSON.name(), testTopic);
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(testSchema, SCHEMA_ID_FOR_TESTING);

        doReturn(genericBytes).
                when(deserializer).getData(jsonBytes);
        doReturn(testSchema).
                when(deserializer).getSchema(jsonBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode(testTopic, testSchema, genericBytes);
        assertEquals(converter.fromConnectData(testTopic, expected.schema(), jsonBytes), ENCODED_DATA);
    }

    /**
     * Test Converter when message without schema is replicated.
     */
    @Test
    public void testConverter_fromConnectData_noSchema_succeeds() throws NoSuchFieldException, IllegalAccessException {
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);

        when(deserializer.getData(genericBytes)).thenThrow(new GlueSchemaRegistryIncompatibleDataException("No schema in message"));
        assertEquals(converter.fromConnectData(testTopic, expected.schema(), genericBytes), genericBytes);
    }

    /**
     * Test Converter when serializer throws exception with protobuf schema.
     */
    @Test
    public void testConverter_fromConnectData_serializer_protobufSchema_throwsException() throws NoSuchFieldException, IllegalAccessException {
        Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", DataFormat.PROTOBUF.name(), "schemaFoo");
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(SCHEMA_REGISTRY_SCHEMA, SCHEMA_ID_FOR_TESTING);

        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        when(serializer.encode(testTopic, SCHEMA_REGISTRY_SCHEMA, USER_DATA)).thenThrow(new AWSSchemaRegistryException());
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when the deserializer throws exception with protobuf schema.
     */
    @Test
    public void testConverter_fromConnectData_deserializer_protobufSchema_throwsException() throws NoSuchFieldException, IllegalAccessException {
        Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", DataFormat.PROTOBUF.name(), "schemaFoo");
        Struct expected = createStructRecord();

        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(SCHEMA_REGISTRY_SCHEMA, SCHEMA_ID_FOR_TESTING);

        when((deserializer).getData(genericBytes)).thenThrow(new AWSSchemaRegistryException());
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode("schemaFoo", SCHEMA_REGISTRY_SCHEMA, USER_DATA);
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when Protobuf schema is replicated.
     */
    @Test
    public void getSchema_protobuf_succeeds() throws NoSuchFieldException, IllegalAccessException, ExecutionException {
        Map<String, String> configs = getTestConfigs();
        Schema testSchema = new Schema("foo", DataFormat.PROTOBUF.name(), testTopic);
        Struct expected = createStructRecord();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);
        converter.schemaDefinitionToVersionCache.put(testSchema, SCHEMA_ID_FOR_TESTING);

        doReturn(genericBytes).
                when(deserializer).getData(protobufBytes);
        doReturn(testSchema).
                when(deserializer).getSchema(protobufBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode(testTopic, testSchema, genericBytes);
        assertEquals(converter.fromConnectData(testTopic, expected.schema(), protobufBytes), ENCODED_DATA);
    }

    /**
     * Test toConnectData when IllegalAccessException is thrown.
     */
    @Test
    public void toConnectData_throwsException() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> configs = getTestConfigs();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);

        assertThrows(UnsupportedOperationException.class, () -> converter.toConnectData(testTopic, genericBytes));
    }



    @Test
    public void testCreateSchemaAndRegisterAllSchemaVersions_nullSchemaDefinition_throwsException() {
        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        Schema schema = new Schema(null, DataFormat.AVRO.name(), "test-schema-name");

        Assertions.assertThrows(NullPointerException.class, () -> converter.createSchemaAndRegisterAllSchemaVersions(schema));
    }

    @Test
    public void testCreateSchemaAndRegisterAllSchemaVersions_nullSchemaName_throwsException() {
        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        Schema schema = new Schema(userSchemaDefinition, DataFormat.AVRO.name(), null);

        Assertions.assertThrows(NullPointerException.class, () -> converter.createSchemaAndRegisterAllSchemaVersions(schema));
    }

    @Test
    public void testCreateSchemaAndRegisterAllSchemaVersions_nullSchemaDataFormat_throwsException() {
        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        Schema schema = new Schema(userSchemaDefinition,null, "test-schema-name");

        Assertions.assertThrows(NullPointerException.class, () -> converter.createSchemaAndRegisterAllSchemaVersions(schema));
    }

    @Test
    public void testCreateSchemaAndRegisterAllSchemaVersions_WhenVersionIsPresent_ReturnsIt() throws Exception {
        Map<String, String> configs = getTestConfigs();

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        mockGetSchemaByDefinition(schemaName, registryName);

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);

        Schema schema = new Schema(userSchemaDefinition, dataFormatName, schemaName);
        UUID schemaVersionId = converter.createSchemaAndRegisterAllSchemaVersions(schema);

        assertEquals(SCHEMA_ID_FOR_TESTING, schemaVersionId);
    }

    @Test
    public void testCreateSchemaAndRegisterAllSchemaVersions_schemaVersionNotPresent_autoRegistersSchemaVersion() throws Exception {
        Map<String, String> configs = getTestConfigs();

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        Long schemaVersionNumber = 1L;
        Long schemaVersionNumber2 = 2L;
        SchemaId requestSchemaId = SchemaId.builder().schemaName(schemaName).registryName(registryName).build();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        mockGetSchema(schemaName, registryName, glueSchemaRegistryConfiguration);
        mockCreateSchema(schemaName, dataFormatName, glueSchemaRegistryConfiguration);
        mockRegisterSchemaVersion(schemaVersionNumber, requestSchemaId);
        mockGetSchemaVersions(schemaVersionNumber, schemaVersionNumber2);
        mockListSchemaVersions(schemaName, registryName, schemaVersionNumber, schemaVersionNumber2);
        mockQuerySchemaVersionMetadata();

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);

        Schema schema = new Schema(userSchemaDefinition, dataFormatName, schemaName);
        UUID schemaVersionId = converter.createSchemaAndRegisterAllSchemaVersions(schema);

        assertEquals(SCHEMA_ID_FOR_TESTING, schemaVersionId);
    }

    @Test
    public void testCreateSchemaAndRegisterAllSchemaVersions_OnUnknownException_ThrowsException() throws Exception {
        Map<String, String> configs = getTestConfigs();

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration awsSchemaRegistrySerDeConfigs = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs);

        mockGetSchemaByDefinition_ThrowException(schemaName, registryName);

        SchemaByDefinitionFetcher schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs);

        Exception exception = assertThrows(AWSSchemaRegistryException.class,
            () -> schemaByDefinitionFetcher.getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata()));
        assertTrue(
            exception.getMessage().contains("Exception occurred while fetching or registering schema definition"));
    }

    @Test
    public void testCreateSchemaAndRegisterAllSchemaVersions_schemaNotPresent_autoCreatesSchemaAndRegisterSchemaVersions_retrieveFromCache() throws Exception {
        Map<String, String> configs = getTestConfigs();

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();
        Long schemaVersionNumber = 1L;
        Long schemaVersionNumber2 = 2L;
        SchemaId requestSchemaId = SchemaId.builder().schemaName(schemaName).registryName(registryName).build();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        sourceSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(sourceSchemaRegistryClient, glueSchemaRegistryConfiguration);
        targetSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(targetSchemaRegistryClient, glueSchemaRegistryConfiguration);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = getSchemaByDefinitionRequest(schemaName, registryName);
        mockGetSchema(schemaName, registryName, glueSchemaRegistryConfiguration);
        mockCreateSchema(schemaName, dataFormatName, glueSchemaRegistryConfiguration);
        mockRegisterSchemaVersion(schemaVersionNumber, requestSchemaId);
        mockGetSchemaVersions(schemaVersionNumber, schemaVersionNumber2);
        mockListSchemaVersions(schemaName, registryName, schemaVersionNumber, schemaVersionNumber2);
        mockQuerySchemaVersionMetadata();

        AWSGlueCrossRegionSchemaReplicationConverter converter = new AWSGlueCrossRegionSchemaReplicationConverter(sourceSchemaRegistryClient, targetSchemaRegistryClient, deserializer, serializer);
        converter.configure(configs, false);

        LoadingCache<Schema, UUID> cache = converter.schemaDefinitionToVersionCache;

        Schema expectedSchema = new Schema(userSchemaDefinition, dataFormatName, schemaName);
        Schema expectedSchema2 = new Schema(userSchemaDefinition2, dataFormatName, schemaName);

        //Ensure cache is empty to start with.
        assertEquals(0, cache.size());

        //First call will create schema and register other schema versions
        Schema schema = new Schema(userSchemaDefinition, dataFormatName, schemaName);
        converter.createSchemaAndRegisterAllSchemaVersions(schema);

        //Ensure cache is populated
        assertEquals(2, cache.size());

        //Ensure corresponding UUID matches with schema
        assertEquals(SCHEMA_ID_FOR_TESTING, cache.get(expectedSchema));
        assertEquals(SCHEMA_ID_FOR_TESTING2, cache.get(expectedSchema2));

        //Second call will be served from cache
        converter.createSchemaAndRegisterAllSchemaVersions(schema);

        //Third call will be served from cache
        schema = new Schema(userSchemaDefinition2, dataFormatName, schemaName);
        converter.createSchemaAndRegisterAllSchemaVersions(schema);

        //Ensure cache is populated
        assertEquals(2, cache.size());

        //Ensure only 1 call happened.
        verify(mockGlueClient, times(1)).getSchemaByDefinition(getSchemaByDefinitionRequest);
    }

    private void mockGetSchemaByDefinition_ThrowException(String schemaName, String registryName) {
        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);

        AWSSchemaRegistryException awsSchemaRegistryException =
                new AWSSchemaRegistryException(new RuntimeException("Unknown"));

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest)).thenThrow(awsSchemaRegistryException);
    }

    private GetSchemaByDefinitionRequest getSchemaByDefinitionRequest(String schemaName, String registryName) {
        return awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);
    }

    private void mockQuerySchemaVersionMetadata() {
        QuerySchemaVersionMetadataRequest querySchemaVersionMetadataRequest = QuerySchemaVersionMetadataRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build();

        QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse = QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .metadataInfoMap(new HashMap<>())
                .build();

        QuerySchemaVersionMetadataRequest querySchemaVersionMetadataRequest2 = QuerySchemaVersionMetadataRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING2.toString())
                .build();

        QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse2 = QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING2.toString())
                .metadataInfoMap(new HashMap<>())
                .build();

        when(mockGlueClient.querySchemaVersionMetadata(querySchemaVersionMetadataRequest)).thenReturn(querySchemaVersionMetadataResponse);
        when(mockGlueClient.querySchemaVersionMetadata(querySchemaVersionMetadataRequest2)).thenReturn(querySchemaVersionMetadataResponse2);
    }

    private void mockGetSchemaVersions(Long schemaVersionNumber, Long schemaVersionNumber2) {
        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString()).build();

        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(userSchemaDefinition)
                .versionNumber(schemaVersionNumber)
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .dataFormat(DataFormat.AVRO)
                .status(String.valueOf(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE))
                .build();

        when(mockGlueClient.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse);

        GetSchemaVersionRequest getSchemaVersionRequest2 = GetSchemaVersionRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING2.toString()).build();

        GetSchemaVersionResponse getSchemaVersionResponse2 = GetSchemaVersionResponse.builder()
                .schemaDefinition(userSchemaDefinition2)
                .versionNumber(schemaVersionNumber2)
                .schemaVersionId(SCHEMA_ID_FOR_TESTING2.toString())
                .dataFormat(DataFormat.AVRO)
                .status(String.valueOf(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE))
                .build();

        when(mockGlueClient.getSchemaVersion(getSchemaVersionRequest2)).thenReturn(getSchemaVersionResponse2);
    }

    private void mockGetSchema(String schemaName, String registryName, GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {
        GetSchemaRequest getSchemaRequest = GetSchemaRequest.builder()
                .schemaId(SchemaId.builder().schemaName(schemaName).registryName(registryName).build())
                .build();

        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .compatibility(Compatibility.FORWARD)
                .schemaName(schemaName)
                .dataFormat(DataFormat.AVRO)
                .registryName(registryName)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .description(glueSchemaRegistryConfiguration.getDescription())
                .build();

        when(mockGlueClient.getSchema(getSchemaRequest)).thenReturn(getSchemaResponse);
    }

    private void mockCreateSchema(String schemaName, String dataFormatName, GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {
        CreateSchemaResponse createSchemaResponse = CreateSchemaResponse.builder()
                .schemaName(schemaName)
                .dataFormat(dataFormatName)
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build();
        CreateSchemaRequest createSchemaRequest = CreateSchemaRequest.builder()
                .dataFormat(DataFormat.AVRO)
                .description(glueSchemaRegistryConfiguration.getDescription())
                .schemaName(schemaName)
                .schemaDefinition(userSchemaDefinition)
                .compatibility(Compatibility.FORWARD)
                .tags(glueSchemaRegistryConfiguration.getTags())
                .registryId(RegistryId.builder().registryName(glueSchemaRegistryConfiguration.getRegistryName()).build())
                .build();

        when(mockGlueClient.createSchema(createSchemaRequest)).thenReturn(createSchemaResponse);
    }

    private void mockListSchemaVersions(String schemaName, String registryName, Long schemaVersionNumber, Long schemaVersionNumber2) {
        ListSchemaVersionsResponse listSchemaVersionsResponse = ListSchemaVersionsResponse.builder()
                .schemas(SchemaVersionListItem.
                                builder().
                                schemaArn("test/"+ schemaName).
                                schemaVersionId(SCHEMA_ID_FOR_TESTING.toString()).
                                versionNumber(schemaVersionNumber).
                                status("CREATED").
                                build(),
                        SchemaVersionListItem.
                                builder().
                                schemaArn("test/"+ schemaName).
                                schemaVersionId(SCHEMA_ID_FOR_TESTING2.toString()).
                                versionNumber(schemaVersionNumber2).
                                status("CREATED").
                                build()
                )
                .nextToken(null)
                .build();
        ListSchemaVersionsRequest listSchemaVersionsRequest = ListSchemaVersionsRequest.builder()
                .schemaId(SchemaId.builder().schemaName(schemaName).registryName(registryName).build())
                .build();

        when(mockGlueClient.listSchemaVersions(listSchemaVersionsRequest)).thenReturn(listSchemaVersionsResponse);
    }

    private void mockRegisterSchemaVersion(Long schemaVersionNumber, SchemaId requestSchemaId) {
        RegisterSchemaVersionRequest registerSchemaVersionRequest = RegisterSchemaVersionRequest.builder()
                .schemaDefinition(userSchemaDefinition2)
                .schemaId(requestSchemaId)
                .build();
        RegisterSchemaVersionResponse registerSchemaVersionResponse = RegisterSchemaVersionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING2.toString())
                .versionNumber(schemaVersionNumber)
                .build();
        when(mockGlueClient.registerSchemaVersion(registerSchemaVersionRequest))
                .thenReturn(registerSchemaVersionResponse);
    }

    private void mockGetSchemaByDefinition(String schemaName, String registryName) {
        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);

        GetSchemaByDefinitionResponse getSchemaByDefinitionResponse =
                GetSchemaByDefinitionResponse
                        .builder()
                        .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                        .status(String.valueOf(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE))
                        .build();

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
                .thenReturn(getSchemaByDefinitionResponse);
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

    /**
     * To create a map of configurations without source region.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoSourceRegionProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_REGION, "us-east-1");

        return props;
    }

    /**
     * To create a map of configurations without source registry.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoSourceRegistryProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION, "us-east-1");
        props.put(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_REGION, "us-east-1");

        return props;
    }

    /**
     * To create a map of configurations without target registry.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoTargetRegistryProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION, "us-east-1");
        props.put(SchemaReplicationSchemaRegistryConstants.AWS_TARGET_REGION, "us-east-1");
        props.put(SchemaReplicationSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");

        return props;
    }

    /**
     * To create a map of configurations without source endpoint.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoSourceEndpointProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");
        props.put(SchemaReplicationSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");

        return props;
    }

    /**
     * To create a map of configurations without source endpoint.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoTargetEndpointProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");
        props.put(SchemaReplicationSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "default-registry");
        props.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_ENDPOINT, "https://test");

        return props;
    }

    /**
     * To create a map of configurations without target region.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoTargetRegionProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");

        return props;
    }

    /**
     * To create a map of configurations without target region, target endpoint and target registry name
     * but is replaced by the provided region, endpoint and registry name config.
     *
     * @return a map of configurations
     */
    private Map<String, String> getPropertiesNoTargetDetails() {
        Map<String, String> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");
        props.put(SchemaReplicationSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_ENDPOINT, "https://test");

        return props;
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private Map<String, String> getTestConfigs() {
        Map<String, String> localConfigs = new HashMap<>();
        localConfigs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        localConfigs.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_ENDPOINT, "https://test");
        localConfigs.put(SchemaReplicationSchemaRegistryConstants.AWS_SOURCE_REGION, "us-east-1");
        localConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        localConfigs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.FORWARD.toString());
        localConfigs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        localConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "User-Topic");
        localConfigs.put(SchemaReplicationSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "User-Topic");
        return localConfigs;
    }

    /**
     * To create Connect Struct record.
     *
     * @return Connect Struct
     */
    private Struct createStructRecord() {
        org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
                .build();
        return new Struct(schema);
    }

    private AWSSchemaRegistryClient configureAWSSchemaRegistryClientWithSerdeConfig(
        AWSSchemaRegistryClient awsSchemaRegistryClient,
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration)
        throws NoSuchFieldException, IllegalAccessException {
        Field serdeConfigField = AWSSchemaRegistryClient.class.getDeclaredField("glueSchemaRegistryConfiguration");
        serdeConfigField.setAccessible(true);
        serdeConfigField.set(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);

        return awsSchemaRegistryClient;
    }
}
