package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;

public interface DataConverter {
    //set the field value using the value returned by toProtobufData below
    void toProtobufData(Descriptors.FileDescriptor fileDescriptor, Schema schema, Object value,
                        Descriptors.FieldDescriptor fieldDescriptor,
        Message.Builder messageBuilder);

    //returns the value from the data conversion
    Object toProtobufData(Descriptors.FileDescriptor fileDescriptor, Schema schema, Object value,
                          Descriptors.FieldDescriptor fieldDescriptor);
}
