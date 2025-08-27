package com.amazonaws.services.schemaregistry.utils;

import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils;
import com.google.protobuf.DescriptorProtos;

public final class ProtobufSchemaParser {
    private static final String RAW_SCHEMA_HEADER = "// Proto schema formatted by Wire, do not edit.\n// Source: \n\n";

    /**
     * Get the Protobuf schema definition string from FileDescriptorProto object
     *
     * @param fileDescriptorProto FileDescriptorProto object to get schema string from
     * @return schema string
     */
    public static String getProtobufSchemaStringFromFileDescriptorProto(
        DescriptorProtos.FileDescriptorProto fileDescriptorProto) {
        String rawSchema = FileDescriptorUtils.fileDescriptorToProtoFile(fileDescriptorProto).toSchema();
        return rawSchema.replace(RAW_SCHEMA_HEADER, "");
    }
}