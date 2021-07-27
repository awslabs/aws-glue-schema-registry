package com.amazonaws.services.schemaregistry.deserializers.protobuf;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.serializers.SerializationDataEncoder;
import com.amazonaws.services.schemaregistry.serializers.protobuf.*;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufWireFormatDecoderTest {
    private GlueSchemaRegistryConfiguration configs = new GlueSchemaRegistryConfiguration(new HashMap<String, String>() {{
        put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
    }});
    private final UUID SCHEMA_VERSION_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private final GlueSchemaRegistryDeserializerDataParser deserializerDataParser =
            GlueSchemaRegistryDeserializerDataParser.getInstance();
    private ProtobufSerializer protobufSerializer = new ProtobufSerializer(configs);
    private ProtobufWireFormatDecoder decoder = new ProtobufWireFormatDecoder(new MessageIndexFinder());
    private SerializationDataEncoder encoder = new SerializationDataEncoder(configs);
    private DynamicMessage dynamicMessage = ProtobufGenerator.createDynamicProtobufRecord();
    private ProtobufTestCase basicTestCase = ProtobufTestCaseReader.getTestCaseByName("Basic.proto");
    private Descriptors.FileDescriptor basicFileDescriptor = basicTestCase.getSchema();
    private ByteBuffer dynamicMessageBuffer =
            ByteBuffer.wrap(encoder.write(protobufSerializer.serialize(dynamicMessage), SCHEMA_VERSION_ID_FOR_TESTING));
    byte[] dynamicMessageData = deserializerDataParser.getPlainData(dynamicMessageBuffer);

    @Test
    public void testDecodeDynamicMessage_ValidInputs_Succeeds() throws IOException {
        Object decoded = decoder.decode(dynamicMessageData, basicFileDescriptor, ProtobufMessageType.DYNAMIC_MESSAGE);
        DynamicMessage decodedDynamicMessage = (DynamicMessage) decoded;
        assertArrayEquals(dynamicMessage.toByteArray(), decodedDynamicMessage.toByteArray());

        //TODO: could not assert equals do to varied descriptor addresses for the two objects
    }

    @Test
    public void testDecodeDynamicMessage_NullInputStream_ThrowsException() {
        Exception ex = assertThrows(IllegalArgumentException.class,
                () -> decoder.decode(null, basicFileDescriptor,
                        ProtobufMessageType.DYNAMIC_MESSAGE));
        assertEquals("data is marked non-null but is null", ex.getMessage());
    }

    @Test
    public void testDecodeDynamicMessage_NullDescriptor_ThrowsException() {
        Exception ex = assertThrows(IllegalArgumentException.class,
                () -> decoder.decode(null, null, ProtobufMessageType.DYNAMIC_MESSAGE));
        assertEquals("data is marked non-null but is null", ex.getMessage());
    }

    @Test
    public void testDecode_DynamicMessage_WhenValidInputsArePassed_Succeeds() throws IOException {
        Object decoded = decoder.decode(dynamicMessageData, basicFileDescriptor, ProtobufMessageType.DYNAMIC_MESSAGE);
        Assertions.assertArrayEquals(dynamicMessageData, protobufSerializer.serialize(decoded));
        //TODO: could not assert equals do to varied descriptor addresses for the two objects
    }

    @Test
    public void testDecode_UnknownMessageTypeValidInputs_ToDynamicMessage_Succeeds() throws IOException {
        Object decoded = decoder.decode(dynamicMessageData, basicFileDescriptor, ProtobufMessageType.UNKNOWN);
        Assertions.assertArrayEquals(dynamicMessageData, protobufSerializer.serialize(decoded));
        //TODO: could not assert equals do to varied descriptor addresses for the two objects
    }

    @Test
    public void testDecode_DynamicMessage_InvalidMessageIndex_ThrowsException() {
        String s = "";
        byte[] invalidData = s.getBytes(StandardCharsets.UTF_8);
        Exception ex = assertThrows(UncheckedIOException.class,
                () -> decoder.decode(invalidData, basicFileDescriptor, ProtobufMessageType.DYNAMIC_MESSAGE));
        assertEquals("com.google.protobuf.InvalidProtocolBufferException: While parsing a protocol message, " +
                "the input ended unexpectedly in the middle of a field.  This could mean either that the input has " +
                "been truncated or that an embedded message misreported its own length.", ex.getMessage());
    }
}
