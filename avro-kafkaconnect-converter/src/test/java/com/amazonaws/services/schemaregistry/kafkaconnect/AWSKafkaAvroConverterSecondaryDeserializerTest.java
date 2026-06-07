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

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData;
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroDataConfig;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AWSKafkaAvroConverter secondary deserializer functionality.
 * Tests the fix for the bug where secondary deserializer data was incorrectly
 * processed through GSR schema extraction.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AWSKafkaAvroConverterSecondaryDeserializerTest {

    @Mock
    private AWSKafkaAvroSerializer mockSerializer;
    @Mock
    private AWSKafkaAvroDeserializer mockDeserializer;
    @Mock
    private GlueSchemaRegistryDeserializationFacade mockDeserializationFacade;
    @Mock
    private AvroData mockAvroData;
    @Mock
    private AwsCredentialsProvider mockCredentialsProvider;
    @Mock
    private GenericRecord mockGenericRecord;
    @Mock
    private SpecificRecord mockSpecificRecord;

    private AWSKafkaAvroConverter converter;
    private static final String TEST_TOPIC = "test-topic";
    private static final byte[] GSR_DATA = new byte[]{3, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}; // 20 bytes, starts with GSR magic byte
    private static final byte[] CONFLUENT_DATA = new byte[]{0, 0, 0, 0, 1, 65}; // 6 bytes, starts with Confluent magic byte
    private static final String AVRO_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA_STRING);

    @BeforeEach
    public void setUp() {
        converter = new AWSKafkaAvroConverter(mockSerializer, mockDeserializer, mockAvroData);
        when(mockDeserializer.getGlueSchemaRegistryDeserializationFacade()).thenReturn(mockDeserializationFacade);
    }

    /**
     * Test successful conversion of GSR data using GSR schema extraction.
     */
    @Test
    public void testToConnectData_GSRData_UsesGSRSchemaExtraction() {
        // Arrange
        when(mockDeserializer.deserialize(TEST_TOPIC, GSR_DATA)).thenReturn(mockGenericRecord);
        when(mockDeserializationFacade.canDeserialize(GSR_DATA)).thenReturn(true);
        when(mockDeserializationFacade.getSchemaDefinition((byte[]) GSR_DATA)).thenReturn(AVRO_SCHEMA_STRING);
        when(mockAvroData.toConnectData(any(Schema.class), eq(mockGenericRecord)))
                .thenReturn(new SchemaAndValue(null, "test-result"));

        // Act
        SchemaAndValue result = converter.toConnectData(TEST_TOPIC, GSR_DATA);

        // Assert
        assertNotNull(result);
        assertEquals("test-result", result.value());
        verify(mockDeserializationFacade).canDeserialize(GSR_DATA);
        verify(mockDeserializationFacade).getSchemaDefinition(eq(GSR_DATA));
        verify(mockAvroData).toConnectData(any(Schema.class), eq(mockGenericRecord));
    }

    /**
     * Test successful conversion of secondary deserializer data using Avro object schema extraction.
     */
    @Test
    public void testToConnectData_SecondaryDeserializerData_UsesAvroObjectSchemaExtraction() {
        // Arrange
        when(mockDeserializer.deserialize(TEST_TOPIC, CONFLUENT_DATA)).thenReturn(mockGenericRecord);
        when(mockDeserializationFacade.canDeserialize(CONFLUENT_DATA)).thenReturn(false);
        when(mockGenericRecord.getSchema()).thenReturn(AVRO_SCHEMA);
        when(mockAvroData.toConnectData(AVRO_SCHEMA, mockGenericRecord))
                .thenReturn(new SchemaAndValue(null, "confluent-result"));

        // Act
        SchemaAndValue result = converter.toConnectData(TEST_TOPIC, CONFLUENT_DATA);

        // Assert
        assertNotNull(result);
        assertEquals("confluent-result", result.value());
        verify(mockDeserializationFacade).canDeserialize(CONFLUENT_DATA);
        verify(mockDeserializationFacade, never()).getSchemaDefinition(any(byte[].class));
        verify(mockGenericRecord).getSchema();
        verify(mockAvroData).toConnectData(AVRO_SCHEMA, mockGenericRecord);
    }

    /**
     * Test that GSR schema extraction failure is properly handled.
     */
    @Test
    public void testToConnectData_GSRSchemaExtractionFails_ThrowsDataException() {
        // Arrange
        when(mockDeserializer.deserialize(TEST_TOPIC, GSR_DATA)).thenReturn(mockGenericRecord);
        when(mockDeserializationFacade.canDeserialize(GSR_DATA)).thenReturn(true);
        when(mockDeserializationFacade.getSchemaDefinition(any(byte[].class)))
                .thenThrow(new AWSSchemaRegistryException("Schema not found"));

        // Act & Assert
        DataException exception = assertThrows(DataException.class, 
                () -> converter.toConnectData(TEST_TOPIC, GSR_DATA));
        assertTrue(exception.getMessage().contains("Failed to extract schema from GSR metadata"));
        assertTrue(exception.getCause() instanceof AWSSchemaRegistryException);
    }

    /**
     * Test extraction of schema from GenericRecord.
     */
    @Test
    public void testExtractSchemaFromAvroObject_GenericRecord_ReturnsSchema() {
        // Arrange
        when(mockGenericRecord.getSchema()).thenReturn(AVRO_SCHEMA);

        // Act
        Schema result = converter.extractSchemaFromAvroObject(mockGenericRecord);

        // Assert
        assertEquals(AVRO_SCHEMA, result);
        verify(mockGenericRecord).getSchema();
    }

    /**
     * Test extraction of schema from SpecificRecord.
     */
    @Test
    public void testExtractSchemaFromAvroObject_SpecificRecord_ReturnsSchema() {
        // Arrange
        when(mockSpecificRecord.getSchema()).thenReturn(AVRO_SCHEMA);

        // Act
        Schema result = converter.extractSchemaFromAvroObject(mockSpecificRecord);

        // Assert
        assertEquals(AVRO_SCHEMA, result);
        verify(mockSpecificRecord).getSchema();
    }

    /**
     * Test that invalid Avro object types throw appropriate exception.
     */
    @Test
    public void testExtractSchemaFromAvroObject_InvalidType_ThrowsDataException() {
        // Arrange
        String invalidObject = "not-an-avro-record";

        // Act & Assert
        DataException exception = assertThrows(DataException.class,
                () -> converter.extractSchemaFromAvroObject(invalidObject));
        assertTrue(exception.getMessage().contains("Deserialized object is not a valid Avro record"));
        assertTrue(exception.getMessage().contains("java.lang.String"));
    }

    /**
     * Test that null Avro object throws appropriate exception.
     */
    @Test
    public void testExtractSchemaFromAvroObject_NullObject_ThrowsDataException() {
        // Act & Assert
        DataException exception = assertThrows(DataException.class,
                () -> converter.extractSchemaFromAvroObject(null));
        assertTrue(exception.getMessage().contains("Deserialized object is not a valid Avro record"));
        assertTrue(exception.getMessage().contains("null"));
    }

    /**
     * Test extractAvroSchema method with GSR data.
     */
    @Test
    public void testExtractAvroSchema_GSRData_CallsGSRSchemaExtraction() {
        // Arrange
        when(mockDeserializationFacade.canDeserialize(GSR_DATA)).thenReturn(true);
        when(mockDeserializationFacade.getSchemaDefinition(any(byte[].class))).thenReturn(AVRO_SCHEMA_STRING);

        // Act
        Schema result = converter.extractAvroSchema(GSR_DATA, mockGenericRecord);

        // Assert
        assertEquals(AVRO_SCHEMA.toString(), result.toString());
        verify(mockDeserializationFacade).canDeserialize(GSR_DATA);
        verify(mockDeserializationFacade).getSchemaDefinition(eq(GSR_DATA));
        verify(mockGenericRecord, never()).getSchema();
    }

    /**
     * Test extractAvroSchema method with secondary deserializer data.
     */
    @Test
    public void testExtractAvroSchema_SecondaryData_CallsAvroObjectSchemaExtraction() {
        // Arrange
        when(mockDeserializationFacade.canDeserialize(CONFLUENT_DATA)).thenReturn(false);
        when(mockGenericRecord.getSchema()).thenReturn(AVRO_SCHEMA);

        // Act
        Schema result = converter.extractAvroSchema(CONFLUENT_DATA, mockGenericRecord);

        // Assert
        assertEquals(AVRO_SCHEMA, result);
        verify(mockDeserializationFacade).canDeserialize(CONFLUENT_DATA);
        verify(mockDeserializationFacade, never()).getSchemaDefinition(any(byte[].class));
        verify(mockGenericRecord).getSchema();
    }

    /**
     * Test that deserializer exceptions are properly propagated.
     */
    @Test
    public void testToConnectData_DeserializerException_ThrowsDataException() {
        // Arrange
        when(mockDeserializer.deserialize(TEST_TOPIC, CONFLUENT_DATA))
                .thenThrow(new AWSSchemaRegistryException("Deserialization failed"));

        // Act & Assert
        DataException exception = assertThrows(DataException.class,
                () -> converter.toConnectData(TEST_TOPIC, CONFLUENT_DATA));
        assertTrue(exception.getMessage().contains("Converting byte[] to Kafka Connect data failed due to serialization error"));
        assertTrue(exception.getCause() instanceof AWSSchemaRegistryException);
    }

    /**
     * Integration test with real GenericRecord to ensure schema extraction works end-to-end.
     */
    @Test
    public void testExtractSchemaFromAvroObject_RealGenericRecord_Success() {
        // Arrange
        GenericRecord realRecord = new GenericData.Record(AVRO_SCHEMA);
        realRecord.put("name", "test-user");

        // Act
        Schema result = converter.extractSchemaFromAvroObject(realRecord);

        // Assert
        assertEquals(AVRO_SCHEMA, result);
        assertEquals("User", result.getName());
        assertEquals(1, result.getFields().size());
        assertEquals("name", result.getFields().get(0).name());
    }

    /**
     * Test null value handling.
     */
    @Test
    public void testToConnectData_NullValue_ReturnsNull() {
        // Act
        SchemaAndValue result = converter.toConnectData(TEST_TOPIC, null);

        // Assert
        assertEquals(SchemaAndValue.NULL, result);
        verifyNoInteractions(mockDeserializer, mockDeserializationFacade, mockAvroData);
    }
}
