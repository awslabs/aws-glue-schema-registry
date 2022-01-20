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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;
import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getPrimitiveSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getPrimitiveTypesData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getProtobufPrimitiveMessage;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getEnumSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getEnumTypeData;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getProtobufEnumMessage;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ProtobufSchemaConverterTest {
    private static final String TOPIC_NAME = "Foo";
    private static final String SCHEMA_NAME = "ProtobufConverterTest";
    private static final String PACKAGE_NAME = "com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3";
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

    private static Stream<Arguments> getFromConnectTestCases() {
        return Stream.of(
            Arguments.of(ToProtobufTestDataGenerator.getPrimitiveTypesData(),
                ToProtobufTestDataGenerator.getPrimitiveSchema(SCHEMA_NAME),
                ToProtobufTestDataGenerator.getProtobufPrimitiveMessage()),
            Arguments.of(ToProtobufTestDataGenerator.getEnumTypeData(),
                ToProtobufTestDataGenerator.getEnumSchema(SCHEMA_NAME),
                ToProtobufTestDataGenerator.getProtobufEnumMessage()),
            Arguments.of(ToProtobufTestDataGenerator.getArrayTypeData(),
                ToProtobufTestDataGenerator.getArraySchema(SCHEMA_NAME),
                ToProtobufTestDataGenerator.getProtobufArrayMessage()),
            Arguments.of(ToProtobufTestDataGenerator.getMapTypeData(),
                ToProtobufTestDataGenerator.getMapSchema(SCHEMA_NAME),
                ToProtobufTestDataGenerator.getProtobufMapMessage())
        );
    }

    private static Stream<Arguments> getToConnectTestCases() {
        return Stream.of(
            Arguments.of(ToConnectTestDataGenerator.getPrimitiveProtobufMessages().get(0),
                ToConnectTestDataGenerator.getPrimitiveSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getPrimitiveTypesData(PACKAGE_NAME)),
            Arguments.of(ToConnectTestDataGenerator.getEnumProtobufMessages().get(0),
                ToConnectTestDataGenerator.getEnumSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getEnumTypeData(PACKAGE_NAME)),
            Arguments.of(ToConnectTestDataGenerator.getArrayProtobufMessages().get(0),
                ToConnectTestDataGenerator.getArraySchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getArrayTypeData(PACKAGE_NAME)),
            Arguments.of(ToConnectTestDataGenerator.getMapProtobufMessages().get(0),
                ToConnectTestDataGenerator.getMapSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getMapTypeData(PACKAGE_NAME))
        );
    }

    @Test
    public void initializesConverter_Successfully() {
        assertDoesNotThrow(() -> new ProtobufSchemaConverter());
    }

    @ParameterizedTest
    @MethodSource("getFromConnectTestCases")
    public void fromConnectData_convertsConnectDataToGSRSerializedProtobufData(
        //TODO: Update this for all types, not just Primitive
        Object connectData, Schema connectSchema, DynamicMessage protobufData) {

        ArgumentCaptor<DynamicMessage> argumentCaptor = ArgumentCaptor.forClass(DynamicMessage.class);
        doReturn(new byte[] {}).when(serializer).serialize(eq(TOPIC_NAME), any());
        protobufSchemaConverter.fromConnectData(TOPIC_NAME, connectSchema, connectData);
        verify(serializer, times(1)).serialize(eq(TOPIC_NAME), argumentCaptor.capture());

        assertEquals(protobufData.toString(), argumentCaptor.getValue().toString());
    }

    @ParameterizedTest
    @MethodSource("getToConnectTestCases")
    public void toConnectData_convertsProtobufSerializedDataToConnectData(
        Message protobufData, Schema connectSchema, Object connectData) {

        final byte[] serializedData = protobufData.toByteArray();

        doReturn(protobufData).when(deserializer).deserialize(TOPIC_NAME, serializedData);

        SchemaAndValue schemaAndValue =
            protobufSchemaConverter.toConnectData(TOPIC_NAME, serializedData);

        SchemaAndValue expectedSchemaAndValue = new SchemaAndValue(connectSchema, connectData);
        assertEquals(expectedSchemaAndValue, schemaAndValue);
    }
}