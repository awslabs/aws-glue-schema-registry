package com.amazonaws.services.schemaregistry.serializer;

import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufWireFormatEncoder;
import com.amazonaws.services.schemaregistry.utils.ProtobufSchemaParser;
import com.google.common.collect.BiMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.Base64;

public class ProtobufPreprocessor {
    private static final ProtobufWireFormatEncoder ENCODER = new ProtobufWireFormatEncoder(new MessageIndexFinder());
    private static final MessageIndexFinder MESSAGE_INDEX_FINDER = new MessageIndexFinder();

    public static String convertBase64SchemaToStringSchema(String base64Schema) throws InvalidProtocolBufferException {
        DescriptorProtos.FileDescriptorProto
            fileDescriptorProto =
            DescriptorProtos.FileDescriptorProto.parseFrom(Base64.getDecoder().decode(base64Schema));

        return ProtobufSchemaParser.getProtobufSchemaStringFromFileDescriptorProto(fileDescriptorProto);
    }

    public static byte[] prefixMessageIndexToBytes(byte[] bytesToEncode, String schemaDef, String descriptorFullname)
        throws Descriptors.DescriptorValidationException, IOException, NoSuchMethodException {
        Descriptors.FileDescriptor fileDescriptor = com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufSchemaParser.parse(schemaDef, "any-name.proto");
        BiMap<Descriptors.Descriptor, Integer> allDescriptor = MESSAGE_INDEX_FINDER.getAll(fileDescriptor);

        for (Descriptors.Descriptor descriptor : allDescriptor.keySet()) {
            if (descriptor.getFullName().equals(descriptorFullname)) {
                return ENCODER.prefixMessageIndexToBytes(bytesToEncode, fileDescriptor, descriptor);
            }
        }
        return bytesToEncode;
    }
}
