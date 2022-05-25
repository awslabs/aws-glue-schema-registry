package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;
import additionalTypes.Decimals;

public class DecimalSchemaTypeConverter implements SchemaTypeConverter  {

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            Schema schema, DescriptorProtos.DescriptorProto.Builder descriptorProto,
            DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        return DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setType(TYPE_MESSAGE)
                .setTypeName(Decimals.getDescriptor().getFullName());
    }
}
