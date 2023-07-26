package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
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
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing RegisterSchema class.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)

public class CrossRegionReplicationMM2ConverterTest {
    @Mock
    private GlueSchemaRegistryDeserializationFacade deserializationFacade;
    @Mock
    private GlueSchemaRegistrySerializationFacade serializationFacade;
    @Mock
    private AwsCredentialsProvider credProvider;
    @Mock
    private GlueSchemaRegistryDeserializerImpl deserializer;
    @Mock
    private GlueSchemaRegistrySerializerImpl serializer;
    private final static byte[] ENCODED_DATA = new byte[] { 8, 9, 12, 83, 82 };
    private final static byte[] USER_DATA = new byte[] { 12, 83, 82 };
    private final static Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", "AVRO", "schemaFoo");
    private static final String testTopic = "User-Topic";

    byte[] genericBytes = new byte[] {3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
            93, -23, -17, 12, 99, 10, 115, 97, 110, 115, 97, -58, 1, 6, 114, 101, 100};


    byte[] avroBytes = new byte[] {3, 0, 84, 24, 47, -109, 37, 124, 74, 77, -100,
            -98, -12, 118, 41, 32, 57, -66, 30, 101, 110, 116, 101, 114, 116, 97, 105, 110, 109, 101, 110,
            116, 95, 50, 0, 0, 0, 0, 0, 0, 20, 64};

    private CrossRegionReplicationMM2Converter converter;



    @BeforeEach
    void setUp() {
        converter = new CrossRegionReplicationMM2Converter(deserializationFacade, serializationFacade, credProvider, deserializer, serializer);
    }

    /**
     * Test for Converter config method.
     */
    @Test
    public void testConverter_configure() {
        converter = new CrossRegionReplicationMM2Converter();
        converter.configure(getTestProperties(), false);
        assertNotNull(converter);
        assertNotNull(converter.getDeserializationFacade());
        assertNotNull(converter.getSerializationFacade());
        assertNotNull(converter.getCredentialsProvider());
        assertNotNull(converter.getSerializer());
        assertNotNull(converter.getDeserializer());
        assertNotNull(converter.isKey());

    }

    /**
     * Test Mm2Converter when it returns byte 0 given the input value is null.
     */
    @Test
    public void testConverter_fromConnectData_returnsByte0() {
        Struct expected = createStructRecord();
        assertEquals(Arrays.toString(converter.fromConnectData(testTopic, expected.schema(), null)), Arrays.toString(new byte[0]));
    }

    /**
     * Test Mm2Converter when serializer throws exception.
     */
    @Test
    public void testConverter_fromConnectData_throwsException() {
        Struct expected = createStructRecord();
        doReturn(USER_DATA)
                .when(deserializer).getData(genericBytes);
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        when(serializer.encode(null, SCHEMA_REGISTRY_SCHEMA, USER_DATA)).thenThrow(new AWSSchemaRegistryException());
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test Mm2Converter when the deserializer throws exception.
     */
    @Test
    public void testConverter_fromConnectData_deserializer_getData_ThrowsException() {
        Struct expected = createStructRecord();
        when((deserializer).getData(genericBytes)).thenThrow(new AWSSchemaRegistryException());
        doReturn(SCHEMA_REGISTRY_SCHEMA)
                .when(deserializer).getSchema(genericBytes);
        doReturn(ENCODED_DATA)
                .when(serializer).encode(null, SCHEMA_REGISTRY_SCHEMA, USER_DATA);
        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), genericBytes));
    }

    /**
     * Test getSchema when NullPointerException is thrown.
     */
    @Test
    public void testGetSchema_nullObject_throwsException(){
        assertThrows(NullPointerException.class, () -> converter.getSchema(null));
    }


    /**
     * Test getSchema when an existing Avro schema is being successfully retrieved.
     */
    @Test
    public void getSchema_avro_succeeds(){

        String schemaDefinition = """
                        {"namespace": "com.amazonaws.services.schemaregistry.serializers.avro",
                            "type": "record",
                            "name": "payment",
                            "fields": [
                                {"name": "id", "type": "string"},
                                {"name": "id_6", "type": "double"}
                            ]}""";
        Schema testSchema = new Schema(schemaDefinition, DataFormat.AVRO.name(), testTopic);

        doReturn(testSchema).
                when(deserializationFacade).getSchema(avroBytes);
        Schema returnedSchema = converter.getSchema(avroBytes);

        assertEquals(returnedSchema.getSchemaDefinition(), testSchema.getSchemaDefinition());
        assertEquals(returnedSchema.getSchemaName(), testSchema.getSchemaName());
        assertEquals(returnedSchema.getDataFormat(), testSchema.getDataFormat());
    }

    /**
     * Test getSchema when an existing JSON schema is being successfully retrieved.
     */
    @Test
    public void getSchema_json_succeeds() {

        String testSchemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                + "\"maximum\":180}},\"additionalProperties\":false}";

        String jsonData = "{\"latitude\":48.858093,\"longitude\":2.294694}";
        byte[] jsonBytes = jsonData.getBytes(StandardCharsets.UTF_8);
        Schema testSchema = new Schema(testSchemaDefinition, DataFormat.JSON.name(), testTopic);

        doReturn(testSchema).
                when(deserializationFacade).getSchema(jsonBytes);
        Schema returnedSchema = converter.getSchema(jsonBytes);

        assertEquals(returnedSchema.getSchemaDefinition(), testSchema.getSchemaDefinition());
        assertEquals(returnedSchema.getSchemaName(), testSchema.getSchemaName());
        assertEquals(returnedSchema.getDataFormat(), testSchema.getDataFormat());
    }

    /**
     * Test getSchema when an existing Protobuf schema is being successfully retrieved.
     */
    @Test
    public void getSchema_protobuf_succeeds(){

        Schema testSchema = new Schema("foo", DataFormat.PROTOBUF.name(), testTopic);
        byte[] protobufBytes = "foo".getBytes(StandardCharsets.UTF_8);

        doReturn(testSchema).
                when(deserializationFacade).getSchema(protobufBytes);
        Schema returnedSchema = converter.getSchema(protobufBytes);

        assertEquals(returnedSchema.getSchemaDefinition(), testSchema.getSchemaDefinition());
        assertEquals(returnedSchema.getSchemaName(), testSchema.getSchemaName());
        assertEquals(returnedSchema.getDataFormat(), testSchema.getDataFormat());
    }

    /**
     * Test registerSchema when NullPointerException is thrown.
     */
    @Test
    public void registerSchema_throwsDataException() {
        assertThrows(DataException.class, () -> converter.registerSchema(null));
    }

    /**
     * Test registerSchema for existing Avro schema.
     */
    @Test
    public void registerSchema_avro_nonExisting_succeeds() {

        String schemaDefinition = """
                        {"namespace": "com.amazonaws.services.schemaregistry.serializers.avro",
                            "type": "record",
                            "name": "payment",
                            "fields": [
                                {"name": "id", "type": "string"},
                                {"name": "id_6", "type": "double"}
                            ]}""";
        UUID avroBytesVersionID = UUID.fromString("54182f93-257c-4a4d-9c9e-f476292039be");

        Schema testSchema = new Schema(schemaDefinition, DataFormat.AVRO.name(), testTopic);
        AWSSerializerInput input = new AWSSerializerInput(testSchema.getSchemaDefinition(), testSchema.getSchemaName(), testSchema.getDataFormat(), null);

        doReturn(avroBytesVersionID).
                when(serializationFacade).getOrRegisterSchemaVersion(input);

        assertEquals(converter.registerSchema(testSchema), avroBytesVersionID);
    }

    /**
     * Test registerSchema for existing JSON schema.
     */
    @Test
    public void registerSchema_json_nonExisting_succeeds() {
        String testSchemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                + "\"maximum\":180}},\"additionalProperties\":false}";
        UUID jsonBytesVersionID = UUID.fromString("afba13fd-7c25-4202-8904-1fab3089faf9");

        Schema testSchema = new Schema(testSchemaDefinition, DataFormat.JSON.name(), "testJson");
        AWSSerializerInput input = new AWSSerializerInput(testSchema.getSchemaDefinition(), testSchema.getSchemaName(), testSchema.getDataFormat(), null);

        doReturn(jsonBytesVersionID).
                when(serializationFacade).getOrRegisterSchemaVersion(input);

        assertEquals(converter.registerSchema(testSchema), jsonBytesVersionID);
    }

    /**
     * Test registerSchema for non-existing Protobuf schema.
     */
    @Test
    public void registerSchema_protobuf_nonExisting_succeeds() {
        String testSchemaDefinition = "foo";
        UUID protobufBytesVersionID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");

        Schema testSchema = new Schema(testSchemaDefinition, DataFormat.PROTOBUF.name(), testTopic);
        AWSSerializerInput input = new AWSSerializerInput(testSchema.getSchemaDefinition(), testSchema.getSchemaName(), testSchema.getDataFormat(), null);

        doReturn(protobufBytesVersionID).
                when(serializationFacade).getOrRegisterSchemaVersion(input);

        assertEquals(converter.registerSchema(testSchema), protobufBytesVersionID);
    }


    /**
     * To create a map of configurations w/o source region.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoSourceProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put("TARGET_REGION", "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }

    /**
     * To create a map of configurations w/o source region.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getNoTargetProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put("SOURCE_REGION", "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getTestProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put("SOURCE_REGION", "us-west-2");
        props.put("TARGET_REGION", "us-east-1");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "default-registry");
        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "t2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.AWS_SRC_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }

    /**
     * To create a Connect Struct record.
     *
     * @return a Connect Struct
     */
    private Struct createStructRecord() {
        org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
                .build();
        return new Struct(schema);
    }
}
