package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.NonNull;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Map;

/**
 * Converts Connect data to Protobuf data according to the Protobuf schema.
 */
public class ConnectDataToProtobufDataConverter {

    public Message convert(
        @NonNull final Descriptors.FileDescriptor fileDescriptor,
        @NonNull final Schema schema,
        @NonNull final Object value) {
        final List<Field> fields = schema.fields();
        final Struct data = (Struct) value;

        //TODO: add caching of fileDescriptor to messages by name map
        Map<String, Descriptors.Descriptor> allMessagesByName = DescriptorTree.parseAllDescriptors(fileDescriptor);
        String pathName = getPathName(fileDescriptor.getPackage(), schema.name());
        Descriptors.Descriptor descriptor = allMessagesByName.get(pathName);
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);

        for (final Field field : fields) {
            final Object fieldValue = data.get(field);

            if (field.schema().type().equals(Schema.Type.MAP)) {
                addMapField(dynamicMessageBuilder, field, fieldValue);
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                Descriptors.FieldDescriptor fieldDescriptor = dynamicMessageBuilder.getDescriptorForType().findFieldByName(field.name());
                Message nestedMessage = convert(fileDescriptor, field.schema(), fieldValue);
                dynamicMessageBuilder.setField(fieldDescriptor, nestedMessage);
            } else {
                addField(dynamicMessageBuilder, field, fieldValue);
            }
        }

        return dynamicMessageBuilder.build();
    }

    private String getPathName(final String packageName, final String schemaName) {
        if (schemaName.startsWith(packageName)) {
            return schemaName.replace(packageName, "");
        }
        return "." + schemaName;
    }

    private void addField(final Message.Builder builder, final Field field, final Object value) {
        final String protobufFieldName = field.name();
        final Descriptors.FieldDescriptor fieldDescriptor =
            builder.getDescriptorForType().findFieldByName(protobufFieldName);
        final Schema schema = field.schema();
        final Schema.Type schemaType = schema.type();

        if (value == null) {
            if (!schema.isOptional()) {
                throw new DataException(
                    String.format("Field data cannot be null for non-optional field. %s: %s", schemaType,
                        protobufFieldName));
            }
            return;
        }

        final DataConverter dataConverter = ConnectDataToProtobufDataConverterFactory.get(schema);

        dataConverter.toProtobufData(schema, value, fieldDescriptor, builder);
    }

    private void addMapField(final Message.Builder builder, final Field field, final Object value) {
        final String protobufFieldName = field.name();
        final Schema schema = field.schema();
        final Descriptors.Descriptor mapDescriptor = builder.getDescriptorForType().findNestedTypeByName(
                ProtobufSchemaConverterUtils.toMapEntryName(protobufFieldName));

        DynamicMessage.Builder mapBuilder = DynamicMessage.newBuilder(mapDescriptor);
        final Descriptors.FieldDescriptor keyFieldDescriptor = mapDescriptor.findFieldByName("key");
        final Descriptors.FieldDescriptor valueFieldDescriptor = mapDescriptor.findFieldByName("value");
        final DataConverter keyDataConverter = ConnectDataToProtobufDataConverterFactory.get(schema.keySchema());
        final DataConverter valueDataConverter = ConnectDataToProtobufDataConverterFactory.get(schema.valueSchema());

        Map<?, ?> map = (Map<?, ?>) value;

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            keyDataConverter.toProtobufData(schema.keySchema(), entry.getKey(), keyFieldDescriptor, mapBuilder);
            valueDataConverter.toProtobufData(schema.valueSchema(), entry.getValue(), valueFieldDescriptor, mapBuilder);

            builder.addRepeatedField(builder.getDescriptorForType().findFieldByName(field.name()),
                    mapBuilder.build());
        }
    }
}
