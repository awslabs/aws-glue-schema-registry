package com.amazonaws.services.schemaregistry.integrationtests.generators;

import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator;
import com.google.protobuf.Message;

import java.util.List;

public class ProtobufSpecificNoneCompatDataGenerator implements TestDataGenerator<Message> {
    @Override
    public List<Message> createRecords() {
        return ProtobufGenerator.getAllPOJOMessages();
    }
}
