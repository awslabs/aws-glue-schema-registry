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

package com.amazonaws.services.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroDataConfig;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for testing AWSKafkaAvroConverter class.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AWSKafkaAvroConverterTest {
    @Mock
    private AvroDataConfig mockAvroDataConfig;
    @Mock
    private SchemaByDefinitionFetcher mockSchemaByDefinitionFetcher;
    @Mock
    private AwsCredentialsProvider mockCredProvider;
    @Mock
    private AWSKafkaAvroSerializer awsKafkaAvroSerializer;
    @Mock
    private AWSKafkaAvroDeserializer awsKafkaAvroDeserializer;

    private AWSKafkaAvroConverter converter;
    private Map<String, Object> configs;
    private AvroData avroData;
    private static final String testTopic = "User-Topic";
    private static final UUID schemaVersionIdForTesting = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final byte[] genericBytes = new byte[] {3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
            93, -23, -17, 12, 99, 10, 115, 97, 110, 115, 97, -58, 1, 6, 114, 101, 100};

    @BeforeEach
    public void setUp() {
        avroData = new AvroData(mockAvroDataConfig);
        configs = getProperties();
    }

    /**
     * Test for AWSKafkaAvroConverter config method.
     */
    @Test
    public void testConverter_configure() {
        converter = new AWSKafkaAvroConverter();
        converter.configure(getProperties(), false);
        assertNotNull(converter);
        assertNotNull(converter.getSerializer());
        assertNotNull(converter.getDeserializer());
        assertNotNull(converter.getAvroData());
    }

    @Test
    public void testConverter_configure_usesStsCredentialsProvider() {
        converter = new AWSKafkaAvroConverter();
        Map<String, Object> props = getProperties();
        props.put("sts.role.arn", "TEST_ROLE_ARN");
        props.put("sts.role.session.name", "TEST_ROLE_SESSION_NAME");
        converter.configure(props, false);
        assertNotNull(converter);
        assertNotNull(converter.getSerializer());
        assertTrue(converter.getSerializer().getCredentialProvider() instanceof StsAssumeRoleCredentialsProvider);
        assertNotNull(converter.getDeserializer());
        assertTrue(converter.getDeserializer().getCredentialProvider() instanceof StsAssumeRoleCredentialsProvider);
        assertNotNull(converter.getAvroData());
    }

    /**
     * Test for Struct record.
     */
    @Test
    public void testConverter_fromConnectData_equalsToConnectData() {
        Struct expected = createStructRecord();
        String avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString();
        Object avroData = this.avroData.fromConnectData(expected.schema(), expected);

        AWSKafkaAvroSerializer awsKafkaAvroSerializer = createSerializer(avroSchemaDefinition, schemaVersionIdForTesting);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = createDeserializer(avroData, genericBytes, avroSchemaDefinition);
        converter = new AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, this.avroData);

        byte[] serializedData = converter.fromConnectData(testTopic, expected.schema(), expected);
        SchemaAndValue structRecord = converter.toConnectData(testTopic, serializedData);

        assertEquals(expected, structRecord.value());
    }

    /**
     * Test AWSKafkaAvroConverter when serializer throws exception.
     */
    @Test
    public void testConverter_fromConnectData_throwsException() {
        Struct expected = createStructRecord();
        String avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString();
        Object avroData = this.avroData.fromConnectData(expected.schema(), expected);

        when(awsKafkaAvroSerializer.serialize(testTopic, avroData)).thenThrow(new AWSSchemaRegistryException());
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = createDeserializer(avroData, genericBytes, avroSchemaDefinition);
        converter = new AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, this.avroData);

        assertThrows(DataException.class, () -> converter.fromConnectData(testTopic, expected.schema(), expected));
    }

    /**
     * Test AWSKafkaAvroConverter when de-serializer throws exception.
     */
    @Test
    public void testConverter_toConnectData_throwsException() {
        Struct expected = createStructRecord();
        String avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString();

        AWSKafkaAvroSerializer awsKafkaAvroSerializer = createSerializer(avroSchemaDefinition, schemaVersionIdForTesting);
        when(awsKafkaAvroDeserializer.deserialize(testTopic, genericBytes)).thenThrow(new AWSSchemaRegistryException());
        converter = new AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, avroData);
        byte[] serializedData = converter.fromConnectData(testTopic, expected.schema(), expected);

        assertThrows(DataException.class, () -> converter.toConnectData(testTopic, serializedData));
    }

    /**
     * To create a AWSKafkaAvroSerializer instance with mocked parameters.
     *
     * @return a mocked AWSKafkaAvroSerializer instance
     */
    private AWSKafkaAvroSerializer createSerializer(String schemaDefinition,
                                                    UUID schemaVersionId) {
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(mockCredProvider)
                        .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                        .build();

        when(mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User-Topic"),
                                                     eq(DataFormat.AVRO.name()), anyMap())).thenReturn(schemaVersionId);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = new AWSKafkaAvroSerializer(mockCredProvider, null);
        awsKafkaAvroSerializer.configure(configs, true);

        awsKafkaAvroSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);

        return awsKafkaAvroSerializer;
    }

    /**
     * To create a AWSKafkaAvroDeserializer instance with mocked parameters.
     *
     * @return a mocked AWSKafkaAvroDeserializer instance
     */
    private AWSKafkaAvroDeserializer createDeserializer(Object record,
                                                        byte[] bytes,
                                                        String schemaDefinition) {
        GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade =
                mock(GlueSchemaRegistryDeserializationFacade.class);
        AWSDeserializerInput awsDeserializerInput = AWSDeserializerInput.builder()
                .buffer(ByteBuffer.wrap(bytes))
                .transportName(testTopic)
                .build();

        when(glueSchemaRegistryDeserializationFacade.deserialize(awsDeserializerInput)).thenReturn(record);
        when(glueSchemaRegistryDeserializationFacade.getSchemaDefinition(bytes)).thenReturn(schemaDefinition);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = new AWSKafkaAvroDeserializer(mockCredProvider, null);
        awsKafkaAvroDeserializer.configure(configs, true);

        awsKafkaAvroDeserializer.setGlueSchemaRegistryDeserializationFacade(glueSchemaRegistryDeserializationFacade);

        return awsKafkaAvroDeserializer;
    }

    /**
     * To create a Connect Struct record.
     *
     * @return a Connect Struct
     */
    private Struct createStructRecord() {
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("favorite_number", Schema.INT32_SCHEMA)
                .field("favorite_color", Schema.STRING_SCHEMA)
                .build();

        Struct structRecord = new Struct(schema)
                .put("name", "sansa")
                .put("favorite_number", 99)
                .put("favorite_color", "red");

        return structRecord;
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configuratons
     */
    private Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }
}
