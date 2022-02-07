package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.*;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;

public class EnumDataConverter implements DataConverter {


    @Override
    public void toProtobufData(final Schema schema,
                                  final Object value,
                                  final Descriptors.FieldDescriptor fieldDescriptor,
                                  final Message.Builder messageBuilder) {

        fieldDescriptor.getType(); //delete if unnecessary later
        messageBuilder.setField(fieldDescriptor, value);

    }
}