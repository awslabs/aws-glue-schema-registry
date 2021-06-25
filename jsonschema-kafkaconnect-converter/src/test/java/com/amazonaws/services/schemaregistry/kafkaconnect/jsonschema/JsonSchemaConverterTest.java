/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.DecimalFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing JsonSchemaConverter class.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class JsonSchemaConverterTest {
    private static final String TEST_TOPIC = "User-Topic";
    private static final String TEST_SCHEMA_ARN =
            "arn:aws:glue:us-east-1:111111111111:schema/registry_name/" + TEST_TOPIC;
    @Mock
    GlueSchemaRegistryKafkaSerializer mockGsrKafkaSerializer;
    @Mock
    GlueSchemaRegistryKafkaDeserializer mockGsrKafkaDeserializer;
    @Mock
    private AWSSchemaRegistryClient mockClient;
    @Mock
    private AwsCredentialsProvider mockCredProvider;
    private JsonSchemaConverter converter;
    private Map<String, Object> configs;

    @BeforeEach
    public void setUp() {
        configs = getProperties();
    }

    /**
     * Test for JsonSchemaConverter config method.
     */
    @Test
    public void testConverter_configure_notNull() {
        converter = new JsonSchemaConverter();
        converter.configure(getProperties(), false);
        assertNotNull(converter);
        assertNotNull(converter.getSerializer());
        assertNotNull(converter.getDeserializer());
        assertNotNull(converter.getConnectSchemaToJsonSchemaConverter());
        assertNotNull(converter.getConnectValueToJsonNodeConverter());
        assertNotNull(converter.getJsonNodeToConnectValueConverter());
        assertNotNull(converter.getJsonSchemaToConnectSchemaConverter());
    }

    @ParameterizedTest
    @MethodSource(value = "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#"
                          + "testSchemaAndValueArgumentsProvider")
    public void testConverter_fromConnectData_equalsToConnectData(org.everit.json.schema.Schema jsonSchema,
                                                                  Schema connectSchema,
                                                                  JsonNode jsonValue,
                                                                  Object connectValue) {
        SchemaAndValue expected = new SchemaAndValue(connectSchema, connectValue);

        UUID testSchemaVersionId = UUID.randomUUID();

        GlueSchemaRegistryKafkaSerializer gsrKafkaSerializer =
                createSerializer(jsonSchema.toString(), testSchemaVersionId);

        GlueSchemaRegistryKafkaDeserializer gsrKafkaDeserializer =
                createDeserializer(jsonSchema.toString(), testSchemaVersionId);

        converter = new JsonSchemaConverter(gsrKafkaSerializer, gsrKafkaDeserializer);
        converter.configure(getProperties(), false);

        byte[] serializedData = converter.fromConnectData(TEST_TOPIC, expected.schema(), expected.value());

        SchemaAndValue actual = converter.toConnectData(TEST_TOPIC, serializedData);

        if (!jsonValue.isNull() || !jsonSchema.hasDefaultValue()) {
            assertEquals(expected.schema(), actual.schema());
        }

        if (expected.value() != null && expected.value()
                .getClass()
                .isArray() && Schema.Type.BYTES.equals(connectSchema.type())) {
            assertArrayEquals((byte[]) expected.value(), (byte[]) actual.value());
        } else if (!jsonValue.isNull() || !jsonSchema.hasDefaultValue()) {
            assertEquals(expected.value(), actual.value());
        }
    }

    @ParameterizedTest
    @MethodSource(value = "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#"
                          + "testSchemaAndValueArgumentsProvider")
    public void testConverter_fromConnectData_throwsException(org.everit.json.schema.Schema jsonSchema,
                                                              Schema connectSchema,
                                                              JsonNode jsonValue,
                                                              Object connectValue) {
        // This test should just validate that the right Exception is thrown
        // Skipping Array because the ordering changes for Arrays that represent maps
        // with "key" and "value"
        if (!jsonValue.isArray() && (!jsonValue.isNull() || !jsonSchema.hasDefaultValue())) {
            SchemaAndValue expected = new SchemaAndValue(connectSchema, connectValue);

            converter = new JsonSchemaConverter(mockGsrKafkaSerializer, mockGsrKafkaDeserializer);
            converter.configure(getProperties(), false);

            Object jsonSchemaWithData = JsonDataWithSchema.builder(jsonSchema.toString(), jsonValue.toString())
                    .build();

            when(mockGsrKafkaSerializer.serialize(TEST_TOPIC, jsonSchemaWithData)).thenThrow(
                    new AWSSchemaRegistryException());

            assertThrows(DataException.class,
                         () -> converter.fromConnectData(TEST_TOPIC, expected.schema(), expected.value()));
        }
    }

    @ParameterizedTest
    @MethodSource(value = "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#"
                          + "testSchemaAndValueArgumentsProvider")
    public void testConverter_toConnectData_throwsException(org.everit.json.schema.Schema jsonSchema,
                                                            Schema connectSchema,
                                                            JsonNode jsonValue,
                                                            Object connectValue) {
        SchemaAndValue expected = new SchemaAndValue(connectSchema, connectValue);

        UUID testSchemaVersionId = UUID.randomUUID();

        GlueSchemaRegistryKafkaSerializer gsrKafkaSerializer =
                createSerializer(jsonSchema.toString(), testSchemaVersionId);

        converter = new JsonSchemaConverter(gsrKafkaSerializer, mockGsrKafkaDeserializer);
        converter.configure(getProperties(), false);

        byte[] serializedData = converter.fromConnectData(TEST_TOPIC, expected.schema(), expected.value());

        when(mockGsrKafkaDeserializer.deserialize(TEST_TOPIC, serializedData)).thenThrow(
                new AWSSchemaRegistryException());

        assertThrows(DataException.class, () -> converter.toConnectData(TEST_TOPIC, serializedData));
    }

    /**
     * To create a GlueSchemaRegistryKafkaSerializer instance with mocked parameters.
     *
     * @return a mocked GlueSchemaRegistryKafkaSerializer instance
     */
    private GlueSchemaRegistryKafkaSerializer createSerializer(String schemaDefinition,
                                                               UUID schemaVersionId) {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(mockCredProvider)
                        .schemaRegistryClient(mockClient)
                        .build();

        Mockito.when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq(TEST_TOPIC),
                                                             eq(DataFormat.JSON.name()), anyMap()))
                .thenReturn(schemaVersionId);
        GlueSchemaRegistryKafkaSerializer gsKafkaSerializer =
                new GlueSchemaRegistryKafkaSerializer(mockCredProvider, null);

        gsKafkaSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);

        return gsKafkaSerializer;
    }

    /**
     * To create a GlueSchemaRegistryKafkaDeserializer instance with mocked parameters.
     *
     * @return a mocked GlueSchemaRegistryKafkaDeserializer instance
     */
    private GlueSchemaRegistryKafkaDeserializer createDeserializer(String schemaDefinition,
                                                                   UUID schemaVersionID) {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(mockCredProvider)
                        .schemaRegistryClient(mockClient)
                        .build();

        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(schemaDefinition)
                .dataFormat(DataFormat.JSON)
                .schemaArn(TEST_SCHEMA_ARN)
                .build();

        when(mockClient.getSchemaVersionResponse(schemaVersionID.toString())).thenReturn(getSchemaVersionResponse);
        GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer =
                new GlueSchemaRegistryKafkaDeserializer(mockCredProvider, null);

        glueSchemaRegistryKafkaDeserializer.setGlueSchemaRegistryDeserializationFacade(
                glueSchemaRegistryDeserializationFacade);

        return glueSchemaRegistryKafkaDeserializer;
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
        props.put(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        props.put(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES,
                  Arrays.asList(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS.name()));
        props.put(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES,
                  Arrays.asList(SerializationFeature.INDENT_OUTPUT.name()));

        return props;
    }
}
