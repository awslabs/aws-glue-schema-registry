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
        messageBuilder.setField(fieldDescriptor, toProtobufData(schema, value, fieldDescriptor));
    }

    @Override
    public Object toProtobufData(final Schema schema, final Object value,
                                 final Descriptors.FieldDescriptor fieldDescriptor) {

       final DataConverter dataConverter = ConnectDataToProtobufDataConverterFactory.get(schema.valueSchema());

        Collection original = (Collection) value;
        List<Object> array = new ArrayList<>();
        for (Object elem : original) {
            Object fieldValue = dataConverter.toProtobufData(schema.valueSchema(), elem, fieldDescriptor);
            array.add(fieldValue);
        }
        return array;
    }
}
