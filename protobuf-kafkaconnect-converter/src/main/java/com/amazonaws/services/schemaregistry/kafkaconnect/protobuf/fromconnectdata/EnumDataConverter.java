package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class EnumDataConverter implements DataConverter {


    @Override
    public void toProtobufData(final Schema schema,
                                  final Object value,
                                  final Descriptors.FieldDescriptor fieldDescriptor,
                                  final Message.Builder messageBuilder) {
        try {
            final Descriptors.EnumValueDescriptor enumValue = fieldDescriptor.getEnumType().findValueByName((value.toString()));
            messageBuilder.setField(fieldDescriptor, enumValue);

        } catch (ClassCastException e) {
            throw new DataException(
                    String.format("Invalid schema type %s for value %s", schema.type(), value.getClass()));
        }
    }
}