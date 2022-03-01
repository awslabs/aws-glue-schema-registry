package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.NonNull;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;

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
        //Assuming the first descriptor is the parent descriptor.
        //TODO: Evaluate if This needs to be updated when structure support is added.
        final DynamicMessage.Builder dynamicMessageBuilder =
            DynamicMessage.newBuilder(fileDescriptor.getMessageTypes().get(0));

        for (final Field field : fields) {
            final Object fieldValue = data.get(field);

            addField(dynamicMessageBuilder, field, fieldValue);
        }

        return dynamicMessageBuilder.build();
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

        final DataConverter dataConverter = ConnectDataToProtobufDataConverterFactory.get(fieldDescriptor);

        dataConverter.toProtobufData(schema, value, fieldDescriptor, builder);
    }
}
