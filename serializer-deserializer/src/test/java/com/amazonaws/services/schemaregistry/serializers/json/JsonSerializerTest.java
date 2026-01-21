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
package com.amazonaws.services.schemaregistry.serializers.json;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.AWS_REGION;
import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES;
import static com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_SELF_REFERENCES;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_SELF_REFERENCES_AS_NULL;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonSerializerTest {
    private static final JsonDataWithSchema GENERIC_TEST_RECORD =
            RecordGenerator.createGenericJsonRecord(RecordGenerator.TestJsonRecord.GEOLOCATION);
    private static final Car SPECIFIC_TEST_RECORD = RecordGenerator.createSpecificJsonRecord();
    private JsonSerializer jsonSerializer =
            new JsonSerializer(new GlueSchemaRegistryConfiguration(new HashMap<String, String>() {{
                put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
            }}));

    @Test
    public void testWrapper_serializeWithGenericRecord_bytesMatch() {
        String jsonPayload = "{\"latitude\":48.858093,\"longitude\":2.294694}";
        byte[] expectedBytes = jsonPayload.getBytes(StandardCharsets.UTF_8);
        byte[] serializedBytes = jsonSerializer.serialize(GENERIC_TEST_RECORD);
        assertArrayEquals(expectedBytes, serializedBytes);
    }

    @Test
    public void testWrapper_serializeWithSpecificRecord_bytesMatch() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] expectedBytes = objectMapper.writeValueAsBytes(SPECIFIC_TEST_RECORD);
        byte[] serializedBytes = jsonSerializer.serialize(SPECIFIC_TEST_RECORD);
        assertArrayEquals(expectedBytes, serializedBytes);
    }

    @Test
    public void testValidate_validatesWrapper_successfully() {
        assertDoesNotThrow(() -> jsonSerializer.validate(GENERIC_TEST_RECORD));
    }

    @Test
    public void testValidate_validatesSpecificRecord_successfully() {
        assertDoesNotThrow(() -> jsonSerializer.validate(SPECIFIC_TEST_RECORD));
    }

    @Test
    public void testValidate_validatesWrapper_ThrowsValidationException() {
        Exception ex = assertThrows(
            AWSSchemaRegistryException.class,
            () -> jsonSerializer.validate(RecordGenerator.createNonSchemaConformantJsonData())
        );

        assertEquals("JSON data validation against schema failed.", ex.getMessage());
    }

    @Test
    public void testValidate_validatesBytes_successfully() {
        byte[] dataBytes = GENERIC_TEST_RECORD.getPayload().getBytes();
        String schemaDefinition = GENERIC_TEST_RECORD.getSchema();

        assertDoesNotThrow(() -> jsonSerializer.validate(schemaDefinition, dataBytes));
    }

    @Test
    public void testValidate_validatesBytes_ThrowsValidationException() {
        JsonDataWithSchema nonSchemaConformantRecord = RecordGenerator.createNonSchemaConformantJsonData();
        byte[] dataBytes = nonSchemaConformantRecord.getPayload().getBytes();
        String schemaDefinition = nonSchemaConformantRecord.getSchema();

        Exception ex =
            assertThrows(AWSSchemaRegistryException.class, () -> jsonSerializer.validate(schemaDefinition, dataBytes));

        assertEquals("JSON data validation against schema failed.", ex.getMessage());
    }

    @Test
    public void testValidate_validatesBytes_NonUTF8EncodingThrowsException() {
        //Encoding as UTF-16LE bytes.
        byte[] dataBytes = GENERIC_TEST_RECORD.getPayload().getBytes(StandardCharsets.UTF_16LE);
        String schemaDefinition = GENERIC_TEST_RECORD.getSchema();

        Exception ex =
            assertThrows(AWSSchemaRegistryException.class, () -> jsonSerializer.validate(schemaDefinition, dataBytes));

        assertEquals("Malformed JSON", ex.getMessage());
    }

    @Test
    public void testWrapper_getSchemaDefinition_matches() {
        String schemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                                  + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                                  + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                                  + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                                  + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                                  + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                                  + "\"maximum\":180}},\"additionalProperties\":false}";
        assertEquals(schemaDefinition, jsonSerializer.getSchemaDefinition(GENERIC_TEST_RECORD));
    }

    @Test
    public void testPojo_getSchemaDefinition_asExpected() {
        String schemaDefinition = "{\"$schema\":\"http://json-schema.org/draft-04/schema#\",\"title\":\"Simple Car "
                                  + "Schema\",\"type\":\"object\",\"additionalProperties\":false,"
                                  + "\"description\":\"This is a car\",\"className\":\"com.amazonaws.services"
                                  + ".schemaregistry.serializers.json.Car\","
                                  + "\"properties\":{\"make\":{\"type\":\"string\"},\"model\":{\"type\":\"string\"},"
                                  + "\"used\":{\"type\":\"boolean\",\"default\":true},"
                                  + "\"miles\":{\"type\":\"integer\",\"maximum\":200000,\"multipleOf\":1000},"
                                  + "\"year\":{\"type\":\"integer\",\"minimum\":2000},"
                                  + "\"purchaseDate\":{\"type\":\"integer\",\"format\":\"utc-millisec\"},"
                                  + "\"listedDate\":{\"type\":\"integer\",\"format\":\"utc-millisec\"},"
                                  + "\"owners\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
                                  + "\"serviceChecks\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}},"
                                  + "\"required\":[\"make\",\"model\",\"used\",\"miles\",\"year\"]}";
        assertEquals(schemaDefinition, jsonSerializer.getSchemaDefinition(SPECIFIC_TEST_RECORD));
    }

    @Test
    public void testGetSchemaDefinition_nullObject_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> jsonSerializer.getSchemaDefinition(null));
    }

    @Test
    public void testSerialize_nullObject_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> jsonSerializer.serialize(null));
    }

    @Test
    public void testSerialize_overridesSerializationFeatureToFalse() {
        Map<String, Object> config = new HashMap<>();
        config.put(JACKSON_SERIALIZATION_FEATURES, singletonMap(FAIL_ON_SELF_REFERENCES.name(), false));
        config.put(AWS_REGION, "us-east-1");


        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(config);
        JsonSerializer jsonSerializer = new JsonSerializer(glueSchemaRegistryConfiguration);

        assertFalse(jsonSerializer.getObjectMapper().isEnabled(FAIL_ON_SELF_REFERENCES));
    }

    @Test
    public void testSerialize_overridesSerializationFeatureToTrue() {
        Map<String, Object> config = new HashMap<>();
        config.put(JACKSON_SERIALIZATION_FEATURES, singletonMap(WRITE_SELF_REFERENCES_AS_NULL.name(), true));
        config.put(AWS_REGION, "us-east-1");


        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(config);
        JsonSerializer jsonSerializer = new JsonSerializer(glueSchemaRegistryConfiguration);

        assertTrue(jsonSerializer.getObjectMapper().isEnabled(WRITE_SELF_REFERENCES_AS_NULL));
    }
}
