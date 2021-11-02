package com.amazonaws.services.schemaregistry.integrationtests.generators;

import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufSchemaParser;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.alltypes.AllTypesSyntax2;
import com.google.common.base.Charsets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ProtobufGenericBackwardDataGenerator implements TestDataGenerator<DynamicMessage> {
    private static final String ALLOWLISTED = "allowListed";
    private static final String BLOCKLISTED = "blockListed";

    @Override
    public List<DynamicMessage> createRecords() throws Exception {
        //Refer to protobufBackwardTestV1.proto
        String messageName = "AllTypes";
        AllTypesSyntax2.AllTypes baseMessage = ProtobufGenerator.ALL_TYPES_MESSAGE_SYNTAX2;
        Descriptors.FileDescriptor v1SchemaFileDescriptor =
            ProtobufSchemaParser.parse(getSchemaDef("protobufBackwardTestV1.proto"), "protobufBackwardTest.proto");
        Descriptors.FileDescriptor v2SchemaFileDescriptor =
            ProtobufSchemaParser.parse(getSchemaDef("protobufBackwardTestV2.proto"), "protobufBackwardTest.proto");

        DynamicMessage v1DynamicMessage1 =
            DynamicMessage.newBuilder(v1SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setInt32Type(123)
                        .setStringType(ALLOWLISTED)
                        .build()
                        .toByteArray()
                )
                .build();
        DynamicMessage v2DynamicMessage1 =
            DynamicMessage.newBuilder(v2SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setInt32Type(456)
                        .setStringType(ALLOWLISTED)
                        .build()
                        .toByteArray()
                )
                .build();

        DynamicMessage v1DynamicMessage2 =
            DynamicMessage.newBuilder(v1SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setInt32Type(56)
                        .setStringType(BLOCKLISTED)
                        .build()
                        .toByteArray()
                )
                .build();

        DynamicMessage v2DynamicMessage2 =
            DynamicMessage.newBuilder(v2SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setFixed32Type(56024)
                        .setStringType(BLOCKLISTED)
                        .build()
                        .toByteArray()
                )
                .build();
        DynamicMessage v2DynamicMessage3 =
            DynamicMessage.newBuilder(v2SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setFixed32Type(24)
                        .setStringType(BLOCKLISTED)
                        .build()
                        .toByteArray()
                )
                .build();

        return Arrays.asList(
            v1DynamicMessage1,
            v2DynamicMessage2,
            v1DynamicMessage2,
            v2DynamicMessage1,
            v2DynamicMessage3
        );
    }

    public static boolean filterRecords(Message value) {
        DynamicMessage message = (DynamicMessage) value;
        Optional<Descriptors.FieldDescriptor> nameField =
            message.getAllFields().keySet().stream().filter(fd -> fd.getName().equals("stringType")).findFirst();
        String name = (String) message.getField(nameField.get());
        return name.equals(ALLOWLISTED);
    }

    private String getSchemaDef(String schemaName) throws IOException {
        Path schemaFilePath = Paths.get("src/test/resources/protobuf/" + schemaName);
        return new String(Files.readAllBytes(schemaFilePath), Charsets.UTF_8);
    }
}
