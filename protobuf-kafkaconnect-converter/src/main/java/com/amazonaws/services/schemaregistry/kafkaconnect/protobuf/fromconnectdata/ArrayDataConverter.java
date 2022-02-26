package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ArrayDataConverter implements DataConverter {

    @Override
    public void toProtobufData(
            final Schema schema,
            final Object value,
            final Descriptors.FieldDescriptor fieldDescriptor,
            final Message.Builder messageBuilder) {

        PrimitiveDataConverter primitiveDataConverter = new PrimitiveDataConverter();

        Collection original = (Collection) value;
        List<Object> array = new ArrayList<>();
        for (Object elem : original) {
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            Object fieldValue = primitiveDataConverter.toProtobufData(valueSchema, elem, fieldDescriptor);
            array.add(fieldValue);
        }
        messageBuilder.setField(fieldDescriptor, array);
    }
}
