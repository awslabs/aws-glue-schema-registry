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
                ToProtobufTestDataGenerator.getPrimitiveSchema("primitiveProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufPrimitiveMessage()),
            Arguments.of(ToProtobufTestDataGenerator.getEnumTypeData(),
                ToProtobufTestDataGenerator.getEnumSchema("enumProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufEnumMessage()),
            Arguments.of(ToProtobufTestDataGenerator.getArrayTypeData(),
                ToProtobufTestDataGenerator.getArraySchema("arrayProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufArrayMessage()),
            Arguments.of(ToProtobufTestDataGenerator.getMapTypeData(),
                ToProtobufTestDataGenerator.getMapSchema("mapProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufMapMessage()),
            Arguments.of(ToProtobufTestDataGenerator.getTimeTypeData(),
                ToProtobufTestDataGenerator.getTimeSchema("timeProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufTimeMessage()),
            Arguments.of(ToProtobufTestDataGenerator.getStructTypeData("NestedType"),
                ToProtobufTestDataGenerator.getStructSchema("NestedType"),
                ToProtobufTestDataGenerator.getProtobufNestedMessage("NestedType")),
            Arguments.of(ToProtobufTestDataGenerator.getOneofTypeData(),
                ToProtobufTestDataGenerator.getOneofSchema("oneofProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufOneofMessage())
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
                ToConnectTestDataGenerator.getMapTypeData(PACKAGE_NAME)),
            Arguments.of(ToConnectTestDataGenerator.getTimeProtobufMessages().get(0),
                ToConnectTestDataGenerator.getTimeSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getTimeTypeData(PACKAGE_NAME)),
            Arguments.of(ToConnectTestDataGenerator.getStructProtobufMessages().get(0),
                ToConnectTestDataGenerator.getStructSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getStructTypeData(PACKAGE_NAME)),
            Arguments.of(ToConnectTestDataGenerator.getOneofProtobufMessages().get(0),
                ToConnectTestDataGenerator.getOneofSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getOneofTypeData(PACKAGE_NAME))
        );
    }

    @Test
    public void initializesConverter_Successfully() {
        assertDoesNotThrow(() -> new ProtobufSchemaConverter());
    }

    @ParameterizedTest
    @MethodSource("getFromConnectTestCases")
    public void fromConnectData_convertsConnectDataToGSRSerializedProtobufData(
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