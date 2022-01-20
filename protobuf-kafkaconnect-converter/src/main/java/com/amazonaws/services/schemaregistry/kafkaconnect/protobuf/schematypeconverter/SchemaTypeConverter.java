package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.schematypeconverter;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;

public interface SchemaTypeConverter {
    DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
        Schema schema,
        DescriptorProtos.DescriptorProto.Builder descriptorProto,
        DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder);
}
