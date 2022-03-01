package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;

public class ArraySchemaTypeConverter implements SchemaTypeConverter {

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            final Schema schema, final DescriptorProtos.DescriptorProto.Builder descriptorProto,
            final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        final SchemaTypeConverter schemaTypeConverter = ConnectToProtobufTypeConverterFactory.get(schema.valueSchema());

        DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = schemaTypeConverter
                .toProtobufSchema(schema.valueSchema(), descriptorProto, fileDescriptorProtoBuilder);
        fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
        return fieldBuilder;
    }
}
