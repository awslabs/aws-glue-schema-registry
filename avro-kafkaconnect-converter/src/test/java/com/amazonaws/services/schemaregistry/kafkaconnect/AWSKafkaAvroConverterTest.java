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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.ASSUME_ROLE_ARN;
import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/my-role";
    private static final String REGION = "us-west-2";

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
     * Test AWSKafkaAvroConverter when value is null.
     */
    @Test
    void testConverter_toConnectData_NullValue() {
        converter = spy(new AWSKafkaAvroConverter());
        assertEquals(SchemaAndValue.NULL, converter.toConnectData(testTopic, null));
    }

    /**
     * Test AWSKafkaAvroConverter with assume role.
     */
    @Test
    void testConverter_configure_invokeAssumeRoleWithCustomSession() {
        configs.put(ASSUME_ROLE_ARN, ROLE_ARN);
        configs.put(ASSUME_ROLE_SESSION_NAME, "my-session");

        converter = spy(new AWSKafkaAvroConverter());
        doReturn(mockCredProvider)
                .when(converter)
                .getCredentialsProvider(anyString(), anyString(), anyString());

        converter.configure(configs, true);

        verify(converter).getCredentialsProvider(ROLE_ARN, "my-session", REGION);
        assertTrue(converter.isKey());
        assertNotNull(converter.getSerializer());
        assertNotNull(converter.getDeserializer());
        assertNotNull(converter.getAvroData());
    }

    /**
     * Test AWSKafkaAvroConverter assume role, default session name.
     */
    @Test
    void testConverter_configure_defaultSessionNameForAssumeRole() {
        configs.put(ASSUME_ROLE_ARN, ROLE_ARN);

        converter = spy(new AWSKafkaAvroConverter());
        doReturn(mockCredProvider)
                .when(converter)
                .getCredentialsProvider(anyString(), anyString(), anyString());

        converter.configure(configs, false);

        verify(converter).getCredentialsProvider(ROLE_ARN, "kafka-connect-session", REGION);
        assertFalse(converter.isKey());
    }

    /**
     * Test AWSKafkaAvroConverter assume role empty.
     */
    @Test
    void testConverter_configure_noAssumeRoleIfArnIsEmpty() {
        configs.put(ASSUME_ROLE_ARN, "");

        converter = spy(new AWSKafkaAvroConverter());
        converter.configure(configs, false);

        verify(converter, never())
                .getCredentialsProvider(anyString(), anyString(), anyString());
    }

    /**
     * Test AWSKafkaAvroConverter assume role null.
     */
    @Test
    void testConverter_configure_noAssumeRoleIfArnIsNotProvided() {
        converter = spy(new AWSKafkaAvroConverter());
        converter.configure(getProperties(), false);

        verify(converter, never())
                .getCredentialsProvider(anyString(), anyString(), anyString());
    }

    /**
     * Test that the fix for secondary deserializer schema extraction works with GSR data.
     * This ensures backward compatibility is maintained.
     */
    @Test
    void testConverter_toConnectData_GSRData_BackwardCompatibility() {
        Struct expected = createStructRecord();
        String avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString();
        Object avroData = this.avroData.fromConnectData(expected.schema(), expected);

        AWSKafkaAvroSerializer awsKafkaAvroSerializer = createSerializer(avroSchemaDefinition, schemaVersionIdForTesting);
        AWSKafkaAvroDeserializer awsKafkaAvroDeserializer = createDeserializer(avroData, genericBytes, avroSchemaDefinition);
        
        // Mock canDeserialize to return true for GSR data
        when(awsKafkaAvroDeserializer.getGlueSchemaRegistryDeserializationFacade().canDeserialize(genericBytes))
                .thenReturn(true);
        
        converter = new AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, this.avroData);

        byte[] serializedData = converter.fromConnectData(testTopic, expected.schema(), expected);
        SchemaAndValue structRecord = converter.toConnectData(testTopic, serializedData);

        assertEquals(expected, structRecord.value());
        
        // Verify that GSR schema extraction was used
        verify(awsKafkaAvroDeserializer.getGlueSchemaRegistryDeserializationFacade()).canDeserialize(genericBytes);
        verify(awsKafkaAvroDeserializer.getGlueSchemaRegistryDeserializationFacade()).getSchemaDefinition(eq(genericBytes));
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
        when(glueSchemaRegistryDeserializationFacade.getSchemaDefinition(eq(bytes))).thenReturn(schemaDefinition);
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

        props.put(AWSSchemaRegistryConstants.AWS_REGION, REGION);
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

        return props;
    }
}
