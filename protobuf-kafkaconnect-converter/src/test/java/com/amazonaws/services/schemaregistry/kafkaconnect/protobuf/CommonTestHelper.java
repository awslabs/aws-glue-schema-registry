package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Map;

public class CommonTestHelper {
    public static Schema createConnectSchema(String name, Map<String, Schema> fieldSchemaMap, Map<String, String> parameters) {
        final SchemaBuilder parentSchemaBuilder = new SchemaBuilder(Schema.Type.STRUCT);
        parentSchemaBuilder.name(name);
        parentSchemaBuilder.parameters(parameters);
        parentSchemaBuilder.version(1);

        fieldSchemaMap
            .forEach(parentSchemaBuilder::field);

        return parentSchemaBuilder.build();
    }
}
