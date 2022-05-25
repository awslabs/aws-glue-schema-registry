package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;

public class StructSchemaTypeConverter implements SchemaTypeConverter  {

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            final Schema schema, final DescriptorProtos.DescriptorProto.Builder descriptorProto,
            final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {
        DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        return fieldBuilder;
    }
}
