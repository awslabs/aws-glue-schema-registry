package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE; //schema.parameters.get(PROTOBUF_TYPE) should be Enum

public class EnumSchemaTypeConverter implements SchemaTypeConverter{

    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(Schema schema, DescriptorProtos.DescriptorProto.Builder descriptorProto, DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        //Checking to make sure the schema information is for Enum
        if (!isEnumType(schema)) {
            throw new IllegalStateException("Enum converter invoked for non-enum type: " + schema.type());
        }

        //Defining the Enum in protobuf schema form
        final Map<String, String> schemaParams = schema.parameters();
        final DescriptorProtos.EnumDescriptorProto.Builder enumDescriptorProtoBuilder =
                DescriptorProtos.EnumDescriptorProto.newBuilder().setName(schemaParams.get("ENUM_NAME"));
        for (Map.Entry<String, String> parameter : schemaParams.entrySet()) {
            String parameterKey = parameter.getKey();
            if (parameterKey.startsWith("PROTOBUF_SCHEMA_ENUM.")) {
                enumDescriptorProtoBuilder.addValue(
                        DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                                .setName(parameterKey.replace("PROTOBUF_SCHEMA_ENUM.", ""))
                                .setNumber(Integer.parseInt(parameter.getValue()))
                                .build()
                );
            }
        }

        //Adding the Enum to the protobuf schema file, and defining a field as Enum
        fileDescriptorProtoBuilder.addEnumType(enumDescriptorProtoBuilder);
        DescriptorProtos.FieldDescriptorProto.Builder enumBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder().setName(schema.name());
        enumBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM);
        enumBuilder.setTypeName(enumDescriptorProtoBuilder.getName());

        return enumBuilder;
    }

    public static boolean isEnumType(Schema schema) {
        final Map<String, String> schemaParams = schema.parameters();

        return Schema.Type.STRING.equals(schema.type())
                && schemaParams != null
                && schemaParams.containsKey(PROTOBUF_TYPE)
                && "PROTOBUF_TYPE_ENUM".equals(schemaParams.get(PROTOBUF_TYPE));
    }


}