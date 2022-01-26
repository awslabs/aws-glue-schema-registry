package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf;

import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getPrimitiveSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getPrimitiveTypesData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getProtobufPrimitiveMessage;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ProtobufSchemaConverterTest {
    private static final String TOPIC_NAME = "Foo";
    private static final com.amazonaws.services.schemaregistry.common.Schema GSR_SCHEMA =
        new com.amazonaws.services.schemaregistry.common.Schema("", DataFormat.PROTOBUF.name(), "TestSchema");
    private ProtobufSchemaConverter protobufSchemaConverter;

    @Mock
    private GlueSchemaRegistryKafkaSerializer serializer;

    @Mock
    private GlueSchemaRegistryKafkaDeserializer deserializer;

    @BeforeEach
    public void setUp() {
        Map<String, ?> config = getSchemaRegistryConfig();
        MockitoAnnotations.initMocks(this);
        protobufSchemaConverter = new ProtobufSchemaConverter(serializer, deserializer);
        protobufSchemaConverter.configure(config, false);

        doNothing().when(serializer).configure(config, false);
        doNothing().when(serializer).setUserAgentApp(UserAgents.KAFKACONNECT);

        doNothing().when(deserializer).configure(config, false);
        doNothing().when(deserializer).setUserAgentApp(UserAgents.KAFKACONNECT);
    }

    private Map<String, ?> getSchemaRegistryConfig() {
        return ImmutableMap.of(
            AWSSchemaRegistryConstants.AWS_REGION, "us-east-1",
            AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name()
        );
    }

    private Schema getConnectSchema() {
        return getPrimitiveSchema("ProtobufConverterTest");
    }

    @Test
    public void initializesConverter_Successfully() {
        assertDoesNotThrow(() -> new ProtobufSchemaConverter());
    }

    @Test
    public void fromConnectData_convertsConnectDataToGSRSerializedProtobufData() {
        //TODO: Update this for all types, not just Primitive
        Object connectData = getPrimitiveTypesData();

        ArgumentCaptor<DynamicMessage> argumentCaptor = ArgumentCaptor.forClass(DynamicMessage.class);
        doReturn(new byte[] {}).when(serializer).serialize(eq(TOPIC_NAME), any());
        protobufSchemaConverter.fromConnectData(TOPIC_NAME, getConnectSchema(), connectData);
        verify(serializer, times(1)).serialize(eq(TOPIC_NAME), argumentCaptor.capture());

        assertEquals(getProtobufPrimitiveMessage().toString(), argumentCaptor.getValue().toString());
    }

    @Test
    public void toConnectData_convertsProtobufSerializedDataToConnectData() {
        //TODO: Update this for all types, not just Primitive
        Message protobufMessage = ToConnectTestDataGenerator.getPrimitiveProtobufMessages().get(0);
        String packageName = protobufMessage.getDescriptorForType().getFile().getPackage();

        final byte[] serializedData = ToConnectTestDataGenerator.getPrimitiveProtobufMessages().get(0).toByteArray();

        doReturn(protobufMessage).when(deserializer).deserialize(TOPIC_NAME, serializedData);

        SchemaAndValue schemaAndValue =
            protobufSchemaConverter.toConnectData(TOPIC_NAME, serializedData);

        SchemaAndValue expectedSchemaAndValue =
            new SchemaAndValue(ToConnectTestDataGenerator.getPrimitiveSchema(packageName), ToConnectTestDataGenerator.getPrimitiveTypesData(packageName));
        assertEquals(expectedSchemaAndValue, schemaAndValue);
    }
}