package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import additionalTypes.Decimals;
import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import metadata.ProtobufSchemaMetadata;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_SCALE_VALUE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;

public class DecimalSchemaTypeConverter implements SchemaTypeConverter  {

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            Schema schema, DescriptorProtos.DescriptorProto.Builder descriptorProto,
            DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        String typeName = getTypeName(Decimals.getDescriptor().getMessageTypes().get(0).getFullName());
        DescriptorProtos.FieldDescriptorProto.Builder builder = DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setType(TYPE_MESSAGE)
                .setTypeName(typeName)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);

        if (schema.parameters().containsKey(DECIMAL_SCALE_VALUE)) {
            DescriptorProtos.FieldOptions.Builder keyOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder();
            keyOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataKey, DECIMAL_SCALE_VALUE);
            builder.mergeOptions(keyOptionsBuilder.build());

            DescriptorProtos.FieldOptions.Builder valueOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder();
            valueOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataValue,
                    schema.parameters().get(DECIMAL_SCALE_VALUE));
            builder.mergeOptions(valueOptionsBuilder.build());
        }

        return builder;
    }
}
