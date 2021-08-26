package com.amazonaws.services.schemaregistry.deserializers.protobuf;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.SerializationDataEncoder;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufSerializer;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufTestCase;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufTestCaseReader;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ProtobufDeserializerTest {
    private static GlueSchemaRegistryConfiguration dynamicMessageConfigs = new GlueSchemaRegistryConfiguration(new HashMap<String, String>() {{
            put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
            put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "DYNAMIC_MESSAGE");
            }});
    private ProtobufDeserializer protobufDynamicMessageDeserializer =
            new ProtobufDeserializer(dynamicMessageConfigs);

    private static GlueSchemaRegistryConfiguration unknownMessageConfigs = new GlueSchemaRegistryConfiguration(new HashMap<String, String>() {{
            put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
            put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "UNKNOWN");
            }});
    private ProtobufDeserializer protobufUnknownMessageTypeDeserializer =
            new ProtobufDeserializer(unknownMessageConfigs);

    private static GlueSchemaRegistryConfiguration pojoMessageConfigs = new GlueSchemaRegistryConfiguration(new HashMap<String, String>() {
        {
            put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
            put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "POJO");
        }});
    private ProtobufDeserializer protobufPojoMessageTypeDeserializer =
            new ProtobufDeserializer(pojoMessageConfigs);

    private static ProtobufSerializer protobufSerializer = new ProtobufSerializer(dynamicMessageConfigs);
    private static final SerializationDataEncoder encoder = new SerializationDataEncoder(dynamicMessageConfigs);
    private static final UUID SCHEMA_VERSION_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");


    private static Stream<Arguments> testDynamicMessageProviderWithMessageIndex0() {
        ProtobufTestCase testCase =
                ProtobufTestCaseReader.getTestCaseByName("Basic.proto");
        DynamicMessage addressDynamicMessage = ProtobufGenerator.createDynamicProtobufRecord();
        ByteBuffer buffer = ByteBuffer.wrap(encoder.write(protobufSerializer.serialize(addressDynamicMessage),
                SCHEMA_VERSION_ID_FOR_TESTING));
        String schema = testCase.getRawSchema();
        return Stream.of(
                Arguments.of(addressDynamicMessage, buffer, schema)
        );
    }

    private static Stream<Arguments> testDynamicMessageProviderWithNonZeroMessageIndex() {
        ProtobufTestCase testCase =
                ProtobufTestCaseReader.getTestCaseByName("ComplexNestingSyntax3.proto");
        DynamicMessage message = ProtobufGenerator.createDynamicNRecord();
        ByteBuffer buffer = ByteBuffer.wrap(encoder.write(protobufSerializer.serialize(message),
                SCHEMA_VERSION_ID_FOR_TESTING));
        String schema = testCase.getRawSchema();
        return Stream.of(
                Arguments.of(message, buffer, schema)
        );
    }

    @Test
    public void testBuilder_Succeeds() {
        ProtobufDeserializer deserializer = ProtobufDeserializer.builder()
                .configs(dynamicMessageConfigs)
                .build();
        assertNotNull(deserializer);
    }

    @Test
    public void testDeserialize_NullArgs_ThrowsException() {
        DynamicMessage dynamicMessage = ProtobufGenerator.createDynamicProtobufRecord();
        ByteBuffer buffer = ByteBuffer.wrap(encoder.write(protobufSerializer.serialize(dynamicMessage),
                SCHEMA_VERSION_ID_FOR_TESTING));
        String schema = ProtobufTestCaseReader.getTestCaseByName("Basic.proto").getRawSchema();

        com.amazonaws.services.schemaregistry.common.Schema schemaObject = new com.amazonaws.services.schemaregistry.common.Schema(
                schema, DataFormat.PROTOBUF.name(), "Basic");

        Exception ex = assertThrows(IllegalArgumentException.class,
                () -> protobufDynamicMessageDeserializer.deserialize(null, schemaObject));
        assertEquals("buffer is marked non-null but is null", ex.getMessage());

        ex = assertThrows(IllegalArgumentException.class,
                () -> protobufDynamicMessageDeserializer.deserialize(buffer, null));
        assertEquals("schema is marked non-null but is null", ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("testDynamicMessageProviderWithMessageIndex0")
    public void testDeserialize_DynamicMessage_Succeeds(DynamicMessage dynamicMessage, ByteBuffer buffer, String schema) {
        com.amazonaws.services.schemaregistry.common.Schema schemaObject = new com.amazonaws.services.schemaregistry.common.Schema(
                schema, DataFormat.PROTOBUF.name(), "Basic");

        Object deserializedObject = protobufDynamicMessageDeserializer.deserialize(buffer, schemaObject);

        assertArrayEquals(protobufSerializer.serialize(dynamicMessage),
                protobufSerializer.serialize(deserializedObject));
        //TODO: could not assert equals do to varied descriptor addresses for the two objects
    }

    @ParameterizedTest
    @MethodSource("testDynamicMessageProviderWithNonZeroMessageIndex")
    public void testDeserialize_DynamicMessageN_Succeeds(DynamicMessage dynamicMessage, ByteBuffer buffer, String schema) {
        com.amazonaws.services.schemaregistry.common.Schema schemaObject = new com.amazonaws.services.schemaregistry.common.Schema(
                schema, DataFormat.PROTOBUF.name(), "Basic");
        Object deserializedObject = protobufDynamicMessageDeserializer.deserialize(buffer, schemaObject);
        assertArrayEquals(protobufSerializer.serialize(dynamicMessage),
                protobufSerializer.serialize(deserializedObject));
        //TODO: could not assert equals do to varied descriptor addresses for the two objects
    }

    @ParameterizedTest
    @MethodSource("testDynamicMessageProviderWithMessageIndex0")
    public void testDeserialize_DynamicMessage_ThrowsExceptionInvalidSchema(
            DynamicMessage dynamicMessage, ByteBuffer buffer, String schema) {
        Exception ex = assertThrows(IllegalArgumentException.class, () -> protobufDynamicMessageDeserializer.deserialize(buffer, null));
        assertEquals("schema is marked non-null but is null", ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("testDynamicMessageProviderWithMessageIndex0")
    public void testDeserialize_DynamicMessage_ThrowsExceptionInvalidBytes(
            DynamicMessage dynamicMessage, ByteBuffer buffer, String schema) {
        com.amazonaws.services.schemaregistry.common.Schema schemaObject = new com.amazonaws.services.schemaregistry.common.Schema(
                schema, DataFormat.PROTOBUF.name(), "Basic");

        String random = "invalid bytes";
        ByteBuffer invalidBytes = ByteBuffer.wrap(random.getBytes(StandardCharsets.UTF_8));
        Exception ex = assertThrows(AWSSchemaRegistryException.class,
                () -> protobufDynamicMessageDeserializer.deserialize(invalidBytes, schemaObject));
        assertEquals("Exception occurred while de-serializing Protobuf message", ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("testDynamicMessageProviderWithMessageIndex0")
    public void testDeserialize_DynamicMessage_UnknownMessageType_Succeeds(
            DynamicMessage dynamicMessage, ByteBuffer buffer, String schema) {
        com.amazonaws.services.schemaregistry.common.Schema schemaObject = new com.amazonaws.services.schemaregistry.common.Schema(
                schema, DataFormat.PROTOBUF.name(), "Basic");

        Object deserializedObject = protobufUnknownMessageTypeDeserializer.deserialize(buffer, schemaObject);

        assertArrayEquals(protobufSerializer.serialize(dynamicMessage),
                protobufSerializer.serialize(deserializedObject));
    }

    @ParameterizedTest
    @MethodSource("testDynamicMessageProviderWithMessageIndex0")
    public void testDeserialize_POJO(DynamicMessage dynamicMessage, ByteBuffer buffer, String schema) {
        com.amazonaws.services.schemaregistry.common.Schema schemaObject = new com.amazonaws.services.schemaregistry.common.Schema(
                schema, DataFormat.PROTOBUF.name(), "Basic");
        Object deserializedObject = protobufPojoMessageTypeDeserializer.deserialize(buffer, schemaObject);

        assertArrayEquals(protobufSerializer.serialize(dynamicMessage),
                protobufSerializer.serialize(deserializedObject));
    }


}
