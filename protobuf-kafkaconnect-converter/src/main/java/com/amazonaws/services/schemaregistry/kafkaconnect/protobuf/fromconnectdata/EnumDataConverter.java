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
        messageBuilder.setField(fieldDescriptor, toProtobufData(schema, value, fieldDescriptor));
    }

    @Override
    public Object toProtobufData(Schema schema, Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getEnumType().findValueByName((value.toString()));
    }
}