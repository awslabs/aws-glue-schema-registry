package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.SchemaV2;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
    private final static byte[] ENCODED_DATA = new byte[] { 8, 9, 12, 83, 82 };
    private final static byte[] USER_DATA = new byte[] { 12, 83, 82 };
    private static final String testTopic = "User-Topic";
    private AWSGlueCrossRegionSchemaReplicationConverter converter;

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
        converter = new AWSGlueCrossRegionSchemaReplicationConverter(credProvider, deserializer, serializer);
    }

    /**
     * Test for Converter config method.
     */
    @Test
    public void testConverter_configure() {
        converter = new AWSGlueCrossRegionSchemaReplicationConverter();
        converter.configure(getTestProperties(), false);
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
    public void testConverter_noTargetDetails_Succeeds(){
        converter = new AWSGlueCrossRegionSchemaReplicationConverter();
        converter.configure(getPropertiesNoTargetDetails(), false);
        assertNotNull(converter.getSerializer());
    }

    /**
     * Test Converter when it returns null given the input value is null.
     */
    @Test
    public void testConverter_fromConnectData_returnsByte0() {
        Struct expected = createStructRecord();
        assertNull(converter.fromConnectData(testTopic, expected.schema(), null));
    }

    /**
     * Test Converter when serializer throws exception with Avro schema.
     */
    @Test
    public void testConverter_fromConnectData_serializer_avroSchema_throwsException() {
        SchemaV2 SCHEMA_REGISTRY_SCHEMA = new SchemaV2("{}", DataFormat.AVRO.name(), "schemaFoo", Compatibility.FORWARD);
        Struct expected = createStructRecord();
        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchemaV2(genericBytes);
        when(serializer.encodeV2(testTopic, SCHEMA_REGISTRY_SCHEMA, USER_DATA)).thenThrow(new AWSSchemaRegistryException());
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when the deserializer throws exception with Avro schema.
     */
    @Test
    public void testConverter_fromConnectData_deserializer_avroSchema_throwsException() {
        SchemaV2 SCHEMA_REGISTRY_SCHEMA = new SchemaV2("{}", DataFormat.AVRO.name(), "schemaFoo", Compatibility.BACKWARD);
        Struct expected = createStructRecord();
        when((deserializer).getData(genericBytes)).thenThrow(new AWSSchemaRegistryException());
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchemaV2(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encodeV2(null, SCHEMA_REGISTRY_SCHEMA, USER_DATA);
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when Avro schema is replicated.
     */
    @Test
    public void testConverter_fromConnectData_avroSchema_succeeds() {
        String schemaDefinition = "{\"namespace\":\"com.amazonaws.services.schemaregistry.serializers.avro\",\"type\":\"record\",\"name\":\"payment\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"id_6\",\"type\":\"double\"}]}";
        SchemaV2 testSchema = new SchemaV2(schemaDefinition, DataFormat.AVRO.name(), testTopic, Compatibility.FORWARD);
        Struct expected = createStructRecord();
        doReturn(genericBytes).
                when(deserializer).getData(avroBytes);
        doReturn(testSchema).
                when(deserializer).getSchemaV2(avroBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encodeV2(testTopic, testSchema, genericBytes);
        assertEquals(converter.fromConnectData(testTopic, expected.schema(), avroBytes), ENCODED_DATA);
    }

    /**
     * Test Converter when serializer throws exception with JSON schema.
     */
    @Test
    public void testConverter_fromConnectData_serializer_jsonSchema_throwsException() {
        SchemaV2 SCHEMA_REGISTRY_SCHEMA = new SchemaV2("{}", DataFormat.JSON.name(), "schemaFoo", Compatibility.BACKWARD);
        Struct expected = createStructRecord();
        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchemaV2(genericBytes);
        when(serializer.encodeV2(testTopic, SCHEMA_REGISTRY_SCHEMA, USER_DATA)).thenThrow(new AWSSchemaRegistryException());
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when the deserializer throws exception with JSON schema.
     */
    @Test
    public void testConverter_fromConnectData_deserializer_jsonSchema_throwsException() {
        SchemaV2 SCHEMA_REGISTRY_SCHEMA = new SchemaV2("{}", DataFormat.JSON.name(), "schemaFoo", Compatibility.BACKWARD);
        Struct expected = createStructRecord();
        when((deserializer).getData(genericBytes)).thenThrow(new AWSSchemaRegistryException());
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchemaV2(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encodeV2("schemaFoo", SCHEMA_REGISTRY_SCHEMA, USER_DATA);
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when JSON schema is replicated.
     */
    @Test
    public void testConverter_fromConnectData_jsonSchema_succeeds() {
        String testSchemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                + "\"maximum\":180}},\"additionalProperties\":false}";
        SchemaV2 testSchema = new SchemaV2(testSchemaDefinition, DataFormat.JSON.name(), testTopic, Compatibility.BACKWARD);
        Struct expected = createStructRecord();
        doReturn(genericBytes).
                when(deserializer).getData(jsonBytes);
        doReturn(testSchema).
                when(deserializer).getSchemaV2(jsonBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encodeV2(testTopic, testSchema, genericBytes);
        assertEquals(converter.fromConnectData(testTopic, expected.schema(), jsonBytes), ENCODED_DATA);
    }

    /**
     * Test Converter when message without schema is replicated.
     */
    @Test
    public void testConverter_fromConnectData_noSchema_succeeds() {
        Struct expected = createStructRecord();
        when(deserializer.getData(genericBytes)).thenThrow(new GlueSchemaRegistryIncompatibleDataException("No schema in message"));
        assertEquals(converter.fromConnectData(testTopic, expected.schema(), genericBytes), genericBytes);
    }

    /**
     * Test Converter when serializer throws exception with protobuf schema.
     */
    @Test
    public void testConverter_fromConnectData_serializer_protobufSchema_throwsException() {
        SchemaV2 SCHEMA_REGISTRY_SCHEMA = new SchemaV2("{}", DataFormat.PROTOBUF.name(), "schemaFoo", Compatibility.FORWARD);
        Struct expected = createStructRecord();
        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchemaV2(genericBytes);
        when(serializer.encodeV2(testTopic, SCHEMA_REGISTRY_SCHEMA, USER_DATA)).thenThrow(new AWSSchemaRegistryException());
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when the deserializer throws exception with protobuf schema.
     */
    @Test
    public void testConverter_fromConnectData_deserializer_protobufSchema_throwsException() {
        SchemaV2 SCHEMA_REGISTRY_SCHEMA = new SchemaV2("{}", DataFormat.PROTOBUF.name(), "schemaFoo", Compatibility.FORWARD);
        Struct expected = createStructRecord();
        when((deserializer).getData(genericBytes)).thenThrow(new AWSSchemaRegistryException());
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchemaV2(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encodeV2("schemaFoo", SCHEMA_REGISTRY_SCHEMA, USER_DATA);
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Converter when Protobuf schema is replicated.
     */
    @Test
    public void getSchema_protobuf_succeeds(){
        SchemaV2 testSchema = new SchemaV2("foo", DataFormat.PROTOBUF.name(), testTopic, Compatibility.FORWARD);
        Struct expected = createStructRecord();
        doReturn(genericBytes).
                when(deserializer).getData(protobufBytes);
        doReturn(testSchema).
                when(deserializer).getSchemaV2(protobufBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encodeV2(testTopic, testSchema, genericBytes);
        assertEquals(converter.fromConnectData(testTopic, expected.schema(), protobufBytes), ENCODED_DATA);
    }

    /**
     * Test toConnectData when IllegalAccessException is thrown.
     */
    @Test
    public void toConnectData_throwsException(){
        assertThrows(UnsupportedOperationException.class, () -> converter.toConnectData(testTopic, genericBytes));
    }

    /**
     * To create a map of configurations without source region.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoSourceRegionProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.AWS_TARGET_REGION, "us-east-1");

        return props;
    }

    /**
     * To create a map of configurations without source registry.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoSourceRegistryProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.AWS_TARGET_REGION, "us-east-1");

        return props;
    }

    /**
     * To create a map of configurations without target registry.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoTargetRegistryProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.AWS_TARGET_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");

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
        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");
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
        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT, "https://test");

        return props;
    }

    /**
     * To create a map of configurations without target region.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoTargetRegionProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");

        return props;
    }

    /**
     * To create a map of configurations without target region, target endpoint and target registry name
     * but is replaced by the provided region, endpoint and registry name config.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getPropertiesNoTargetDetails() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT, "https://test");

        return props;
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getTestProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_TARGET_REGION, "us-east-1");
        props.put(AWSSchemaRegistryConstants.SOURCE_REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.TARGET_REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.AWS_SOURCE_ENDPOINT, "https://test");

        return props;
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
}
