package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;

public class ProtobufSchemaConverterTest {
    private static final String TOPIC_NAME = "";
    private static final com.amazonaws.services.schemaregistry.common.Schema GSR_SCHEMA =
        new com.amazonaws.services.schemaregistry.common.Schema("", DataFormat.PROTOBUF.name(), "TestSchema");
    private ProtobufSchemaConverter protobufSchemaConverter;

    @BeforeEach
    public void setUp() {
        protobufSchemaConverter = new ProtobufSchemaConverter();
        protobufSchemaConverter.configure(getSchemaRegistryConfig(), false);
    }

    private Map<String, ?> getSchemaRegistryConfig() {
        return ImmutableMap.of(
            AWSSchemaRegistryConstants.AWS_REGION, "us-east-1",
            AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name()
        );
    }

    private Schema getConnectSchema() {
        SchemaBuilder schemaBuilder = new SchemaBuilder(org.apache.kafka.connect.data.Schema.Type.STRUCT);
        schemaBuilder.name("ProtobufConverterTest");
        schemaBuilder.field("TestStringField", new SchemaBuilder(Schema.Type.STRING));
        return schemaBuilder.build();
    }

    @Test
    void fromConnectData_convertsConnectDataToGSRSerializedProtobufData() {
        String connectData = "update-this";
        byte[] serializedData = protobufSchemaConverter.fromConnectData(TOPIC_NAME, getConnectSchema(), connectData);
        //TBD: Update this test
        assertNull(serializedData);
    }

    @Test
    void toConnectData_convertsProtobufSerializedDataToConnectData() {
        final byte[] serializedData = new byte[] {};
        SchemaAndValue schemaAndValue =
            protobufSchemaConverter.toConnectData(TOPIC_NAME, serializedData);
        //TBD: Update this test
        assertNull(schemaAndValue);
    }
}