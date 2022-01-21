package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;

public interface DataConverter {
    void toProtobufData(Schema schema, Object value, Descriptors.FieldDescriptor fieldDescriptor,
        Message.Builder messageBuilder);
}
