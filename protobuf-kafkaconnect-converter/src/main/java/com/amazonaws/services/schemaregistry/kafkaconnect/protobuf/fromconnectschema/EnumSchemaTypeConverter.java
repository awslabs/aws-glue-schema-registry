package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_NAME;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_VALUE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName;

public class EnumSchemaTypeConverter implements SchemaTypeConverter {

    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            Schema schema, DescriptorProtos.DescriptorProto.Builder descriptorProto,
            DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        //Defining the Enum in protobuf schema form
        final Map<String, String> schemaParams = schema.parameters();
        final String enumName = schemaParams.get(PROTOBUF_ENUM_NAME);
        final DescriptorProtos.EnumDescriptorProto.Builder enumDescriptorProtoBuilder =
                DescriptorProtos.EnumDescriptorProto.newBuilder().setName(enumName);
        for (Map.Entry<String, String> parameter : schemaParams.entrySet()) {
            String parameterKey = parameter.getKey();
            if (parameterKey.startsWith(PROTOBUF_ENUM_VALUE)) {
                enumDescriptorProtoBuilder.addValue(
                        DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                                .setName(parameterKey.replace(PROTOBUF_ENUM_VALUE, ""))
                                .setNumber(Integer.parseInt(parameter.getValue()))
                                .build()
                );
            }
        }

        //Adding the Enum to the protobuf schema file, and defining a field as Enum
        fileDescriptorProtoBuilder.addEnumType(enumDescriptorProtoBuilder);
        DescriptorProtos.FieldDescriptorProto.Builder enumBuilder =
                DescriptorProtos.FieldDescriptorProto.newBuilder().setName(enumName);
        enumBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM);
        enumBuilder.setTypeName(
                getTypeName(fileDescriptorProtoBuilder.getPackage() + "." + enumDescriptorProtoBuilder.getName()));
        enumBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);

        return enumBuilder;
    }
}