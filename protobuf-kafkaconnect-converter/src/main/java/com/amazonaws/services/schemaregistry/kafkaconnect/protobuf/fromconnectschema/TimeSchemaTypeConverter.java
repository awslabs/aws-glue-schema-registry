package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;

public class TimeSchemaTypeConverter implements SchemaTypeConverter {

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            Schema schema, DescriptorProtos.DescriptorProto.Builder descriptorProto,
            DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        String typename = "";
        if (Date.SCHEMA.name().equals(schema.name())) {
            typename = com.google.type.Date.getDescriptor().getFullName();
        } else if (Timestamp.SCHEMA.name().equals(schema.name())) {
            typename = com.google.protobuf.Timestamp.getDescriptor().getFullName();
        } else if (Time.SCHEMA.name().equals(schema.name())) {
            typename = com.google.type.TimeOfDay.getDescriptor().getFullName();
        }

        return DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setType(TYPE_MESSAGE)
                .setTypeName(typename);
    }
}
