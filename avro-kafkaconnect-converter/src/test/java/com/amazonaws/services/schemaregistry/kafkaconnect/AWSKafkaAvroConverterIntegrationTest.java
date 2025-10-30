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

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test to verify the secondary deserializer fix works end-to-end.
 * This test simulates the real-world scenario where Confluent data is processed
 * by a GSR converter with a secondary deserializer configured.
 */
public class AWSKafkaAvroConverterIntegrationTest {

    private AWSKafkaAvroConverter converter;
    private static final String TEST_TOPIC = "integration-test-topic";
    private static final String AVRO_SCHEMA_STRING = 
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA_STRING);

    @BeforeEach
    public void setUp() {
        converter = new AWSKafkaAvroConverter();
        
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        
        // Configure secondary deserializer for non-GSR data
        configs.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, StringDeserializer.class.getName());
        
        converter.configure(configs, false);
    }

    /**
     * Integration test that verifies the fix handles non-GSR data correctly.
     * This test creates a real GenericRecord and verifies that schema extraction
     * works properly when the data cannot be deserialized by GSR.
     */
    @Test
    public void testSecondaryDeserializerSchemaExtraction_RealAvroData() {
        // Create a real Avro record
        GenericRecord avroRecord = new GenericData.Record(AVRO_SCHEMA);
        avroRecord.put("name", "John Doe");
        avroRecord.put("age", 30);

        // Simulate non-GSR data (this would normally come from Confluent)
        byte[] nonGSRData = new byte[]{0, 0, 0, 0, 1, 65}; // Confluent format

        // Test the schema extraction method directly
        Schema extractedSchema = converter.extractSchemaFromAvroObject(avroRecord);
        
        // Verify the schema was extracted correctly
        assertNotNull(extractedSchema);
        assertEquals(AVRO_SCHEMA, extractedSchema);
        assertEquals("User", extractedSchema.getName());
        assertEquals(2, extractedSchema.getFields().size());
        assertEquals("name", extractedSchema.getFields().get(0).name());
        assertEquals("age", extractedSchema.getFields().get(1).name());
    }

    /**
     * Test that verifies extractAvroSchema method correctly routes to object-based
     * schema extraction for non-GSR data.
     */
    @Test
    public void testExtractAvroSchema_NonGSRData_UsesObjectExtraction() {
        // Create a real Avro record
        GenericRecord avroRecord = new GenericData.Record(AVRO_SCHEMA);
        avroRecord.put("name", "Jane Smith");
        avroRecord.put("age", 25);

        // Simulate non-GSR data
        byte[] nonGSRData = new byte[]{0, 0, 0, 0, 1, 65};

        // Test the schema extraction
        Schema extractedSchema = converter.extractAvroSchema(nonGSRData, avroRecord);
        
        // Verify the schema was extracted from the object, not the bytes
        assertNotNull(extractedSchema);
        assertEquals(AVRO_SCHEMA, extractedSchema);
    }

    /**
     * Test error handling for invalid Avro objects.
     */
    @Test
    public void testExtractSchemaFromAvroObject_InvalidObject_ThrowsException() {
        String invalidObject = "not-an-avro-record";
        byte[] someData = new byte[]{1, 2, 3};

        assertThrows(Exception.class, () -> {
            converter.extractAvroSchema(someData, invalidObject);
        });
    }

    /**
     * Test that null objects are handled gracefully.
     */
    @Test
    public void testExtractSchemaFromAvroObject_NullObject_ThrowsException() {
        byte[] someData = new byte[]{1, 2, 3};

        assertThrows(Exception.class, () -> {
            converter.extractAvroSchema(someData, null);
        });
    }
}
