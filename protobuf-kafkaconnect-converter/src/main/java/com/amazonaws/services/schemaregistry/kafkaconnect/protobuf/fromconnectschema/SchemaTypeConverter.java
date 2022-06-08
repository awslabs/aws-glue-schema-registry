package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;

public interface SchemaTypeConverter {
    DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
        Schema schema,
        DescriptorProtos.DescriptorProto.Builder descriptorProto,
        DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder);

    default void addImportToProtobufSchema(DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder, String importFile) {
        if (!fileDescriptorProtoBuilder.getDependencyList().contains(importFile)) {
            fileDescriptorProtoBuilder.addDependency(importFile);
        }
    }
}
