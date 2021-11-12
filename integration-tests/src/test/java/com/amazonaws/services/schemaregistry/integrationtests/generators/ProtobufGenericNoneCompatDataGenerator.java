package com.amazonaws.services.schemaregistry.integrationtests.generators;

import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator;
import com.google.protobuf.DynamicMessage;

import java.util.List;

public class ProtobufGenericNoneCompatDataGenerator implements TestDataGenerator<DynamicMessage> {
    @Override
    public List<DynamicMessage> createRecords() {
        return ProtobufGenerator.getAllDynamicMessages();
    }
}
