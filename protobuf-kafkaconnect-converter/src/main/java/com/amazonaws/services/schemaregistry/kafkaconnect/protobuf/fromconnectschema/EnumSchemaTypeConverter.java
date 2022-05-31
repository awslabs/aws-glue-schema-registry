package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_NAME;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getSchemaSimpleName;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName;

public class EnumSchemaTypeConverter implements SchemaTypeConverter {

    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            Schema schema, DescriptorProtos.DescriptorProto.Builder descriptorProto,
            DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        //Defining the Enum in protobuf schema form
        final Map<String, String> schemaParams = schema.parameters();
        final String enumFullName = schemaParams.get(PROTOBUF_ENUM_NAME);
        final String enumName = getSchemaSimpleName(enumFullName);

        DescriptorProtos.FieldDescriptorProto.Builder enumBuilder =
                DescriptorProtos.FieldDescriptorProto.newBuilder().setName(enumName);
        enumBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM);
        enumBuilder.setTypeName(getTypeName(enumFullName));
        enumBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);

        return enumBuilder;
    }
}