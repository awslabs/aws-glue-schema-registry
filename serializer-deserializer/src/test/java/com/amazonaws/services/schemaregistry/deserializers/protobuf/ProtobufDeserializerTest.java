package com.amazonaws.services.schemaregistry.deserializers.protobuf;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.SerializationDataEncoder;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.BASIC_REFERENCING_DYNAMIC_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.BASIC_SYNTAX2_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.BASIC_SYNTAX3_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.DOLLAR_SYNTAX_3_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.NESTING_MESSAGE_PROTO2;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.NESTING_MESSAGE_PROTO3;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.SPECIAL_CHARS_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.UNICODE_MESSAGE;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator.createDynamicProtobufRecord;
import static com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufTestCaseReader.getTestCaseByName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufDeserializerTest {
    public static final String SCHEMA_NAME = "Basic";
    private static final ByteBuffer ANY_BUFFER = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
    private static final Schema ANY_SCHEMA =
        new com.amazonaws.services.schemaregistry.common.Schema("foo", DataFormat.PROTOBUF.name(), SCHEMA_NAME);
    private static GlueSchemaRegistryConfiguration dynamicMessageConfigs =
        new GlueSchemaRegistryConfiguration(ImmutableMap.of(
            AWSSchemaRegistryConstants.AWS_REGION, "us-west-2",
            AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "DYNAMIC_MESSAGE"
        ));
    private final ProtobufDeserializer protobufDynamicMessageDeserializer =
            new ProtobufDeserializer(dynamicMessageConfigs);

    private static GlueSchemaRegistryConfiguration pojoMessageConfigs =
        new GlueSchemaRegistryConfiguration(ImmutableMap.of(
            AWSSchemaRegistryConstants.AWS_REGION, "us-west-2",
            AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "POJO"
        ));
    private final ProtobufDeserializer protobufPojoMessageTypeDeserializer =
            new ProtobufDeserializer(pojoMessageConfigs);

    private static ProtobufSerializer protobufSerializer = new ProtobufSerializer(dynamicMessageConfigs);
    private static final SerializationDataEncoder SERIALIZATION_DATA_ENCODER = new SerializationDataEncoder(dynamicMessageConfigs);
    private static final UUID SCHEMA_VERSION_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");

    private static Stream<Arguments> testDeserializationMessageProvider() {
        return Stream.of(
            Arguments.of(
                NESTING_MESSAGE_PROTO2, getTestCaseByName("ComplexNestingSyntax2.proto").getRawSchema(), "ComplexNestingSyntax2.proto"
            ),
            Arguments.of(
                NESTING_MESSAGE_PROTO3, getTestCaseByName("ComplexNestingSyntax3.proto").getRawSchema(), "ComplexNestingSyntax3.proto"
            ),
            Arguments.of(
                BASIC_REFERENCING_DYNAMIC_MESSAGE, getTestCaseByName("Basic.proto").getRawSchema(), "Basic.proto"
            ),
            Arguments.of(
                BASIC_SYNTAX3_MESSAGE, getTestCaseByName("basicsyntax3.proto").getRawSchema(), "basicsyntax3"
            ),
            Arguments.of(
                BASIC_SYNTAX2_MESSAGE, getTestCaseByName("basicSyntax2.proto").getRawSchema(), "basicSyntax2"
            ),
            Arguments.of(
                DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE, getTestCaseByName(".protodevelasl.proto.proto.protodevel#$---$#$#.bar.3#.proto").getRawSchema(), ".protodevelasl.proto.proto.protodevel#$---$#$#.bar.3#"
            ),
            Arguments.of(
                UNICODE_MESSAGE, getTestCaseByName("◉◉◉unicode⏩.proto").getRawSchema(), "◉◉◉unicode⏩.proto"
            ),
            Arguments.of(
                SPECIAL_CHARS_MESSAGE, getTestCaseByName("*&()^#!`~;:\"'{[}}<,>>.special?.proto").getRawSchema(), "*&()^#!`~;:\"'{[}}<,>>.special?"
            ),
            Arguments.of(
                DOLLAR_SYNTAX_3_MESSAGE, getTestCaseByName("foo$$$1.proto").getRawSchema(), "foo$$$1.proto"
            )
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
        DynamicMessage dynamicMessage = createDynamicProtobufRecord();
        ByteBuffer buffer = ByteBuffer.wrap(SERIALIZATION_DATA_ENCODER.write(protobufSerializer.serialize(dynamicMessage),
                SCHEMA_VERSION_ID_FOR_TESTING));
        String schema = getTestCaseByName("Basic.proto").getRawSchema();

        com.amazonaws.services.schemaregistry.common.Schema schemaObject =
            new com.amazonaws.services.schemaregistry.common.Schema(
                schema, DataFormat.PROTOBUF.name(), SCHEMA_NAME);

        Exception ex = assertThrows(IllegalArgumentException.class,
                () -> protobufDynamicMessageDeserializer.deserialize(null, schemaObject));
        assertEquals("buffer is marked non-null but is null", ex.getMessage());

        ex = assertThrows(IllegalArgumentException.class,
                () -> protobufDynamicMessageDeserializer.deserialize(buffer, null));
        assertEquals("schema is marked non-null but is null", ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("testDeserializationMessageProvider")
    public void testDeserialize_DynamicMessage_Succeeds(Message message, String schemaDef, String schemaName) {
        byte[] serializedData = protobufSerializer.serialize(message);
        com.amazonaws.services.schemaregistry.common.Schema schemaObject =
            new com.amazonaws.services.schemaregistry.common.Schema(
                schemaDef, DataFormat.PROTOBUF.name(), schemaName);
        ByteBuffer byteBuffer =
            ByteBuffer.wrap(SERIALIZATION_DATA_ENCODER.write(serializedData, SCHEMA_VERSION_ID_FOR_TESTING));

        DynamicMessage deserializedObject =
            (DynamicMessage) protobufDynamicMessageDeserializer.deserialize(byteBuffer, schemaObject);

        assertArrayEquals(message.toByteArray(), deserializedObject.toByteArray());
    }

    @ParameterizedTest
    @MethodSource("testDeserializationMessageProvider")
    public void testDeserialize_WhenDeserializedToPOJO_Succeeds(Message message, String schemaDef, String schemaName) {
        byte[] serializedData = protobufSerializer.serialize(message);
        com.amazonaws.services.schemaregistry.common.Schema schemaObject =
            new com.amazonaws.services.schemaregistry.common.Schema(
                schemaDef, DataFormat.PROTOBUF.name(), schemaName);
        ByteBuffer byteBuffer =
            ByteBuffer.wrap(SERIALIZATION_DATA_ENCODER.write(serializedData, SCHEMA_VERSION_ID_FOR_TESTING));

        Message deserializedObject =
            (Message) protobufPojoMessageTypeDeserializer.deserialize(byteBuffer, schemaObject);

        assertArrayEquals(message.toByteArray(), deserializedObject.toByteArray());
    }

    @Test
    public void testDeserialize_DynamicMessage_ThrowsExceptionInvalidSchema() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> protobufDynamicMessageDeserializer.deserialize(ANY_BUFFER, null));
        assertEquals("schema is marked non-null but is null", ex.getMessage());
    }

    @Test
    public void testDeserialize_DynamicMessage_ThrowsExceptionInvalidBytes() {
        String random = "invalid bytes";
        ByteBuffer invalidBytes = ByteBuffer.wrap(random.getBytes(StandardCharsets.UTF_8));
        Exception ex = assertThrows(AWSSchemaRegistryException.class,
            () -> protobufDynamicMessageDeserializer.deserialize(invalidBytes, ANY_SCHEMA));
        assertEquals("Exception occurred while de-serializing Protobuf message", ex.getMessage());
    }
}
