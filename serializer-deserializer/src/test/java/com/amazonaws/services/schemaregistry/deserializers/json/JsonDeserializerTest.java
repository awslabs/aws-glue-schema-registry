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
package com.amazonaws.services.schemaregistry.deserializers.json;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.json.Car;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.services.schemaregistry.deserializers.json.JsonDeserializer.MAX_WARNED_CLASS_NAMES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonDeserializerTest {
    private static final String GEOLOCATION_SCHEMA =
            "{\"$id\":\"https://example.com/geographical-location.schema.json\","
            + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
            + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
            + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
            + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
            + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
            + "\"maximum\":180}},\"additionalProperties\":false}";
    private static final String GEOLOCATION_PAYLOAD = "{\"latitude\":48.858093,\"longitude\":2.294694}";

    private static final String CAR_SCHEMA =
            "{\"$schema\":\"http://json-schema.org/draft-04/schema#\",\"title\":\"Simple Car "
            + "Schema\",\"type\":\"object\",\"additionalProperties\":false,"
            + "\"description\":\"This is a car\",\"className\":\"com.amazonaws.services"
            + ".schemaregistry.serializers.json.Car\","
            + "\"properties\":{\"make\":{\"type\":\"string\"},\"model\":{\"type\":\"string\"}}}";
    private static final String CAR_PAYLOAD = "{\"make\":\"Honda\",\"model\":\"Civic\"}";

    private final JsonDeserializer jsonDeserializer = new JsonDeserializer(null);

    /**
     * Wraps the raw payload bytes in the Glue Schema Registry header so they can be
     * fed to {@link JsonDeserializer#deserialize}.
     */
    private static ByteBuffer toSerializedBuffer(String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        ByteBuffer byteBuffer = ByteBuffer.allocate(18 + data.length);
        byteBuffer.put(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE);
        byteBuffer.put(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE);
        // Schema version id (128-bit UUID) - value irrelevant for these tests.
        byteBuffer.putLong(0L);
        byteBuffer.putLong(0L);
        byteBuffer.put(data);
        byteBuffer.rewind();
        return byteBuffer;
    }

    private static JsonDeserializer deserializerWithClassNameResolution(boolean enabled) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        configs.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED, String.valueOf(enabled));
        return new JsonDeserializer(new GlueSchemaRegistryConfiguration(configs));
    }

    /**
     * Builds a minimal JSON schema carrying the given {@code className}, to exercise the
     * deserializer against class names a producer could have put in the schema.
     */
    private static String schemaWithClassName(String className) {
        return "{\"$schema\":\"http://json-schema.org/draft-04/schema#\",\"title\":\"Simple Car "
               + "Schema\",\"type\":\"object\",\"additionalProperties\":false,"
               + "\"className\":\"" + className + "\","
               + "\"properties\":{\"make\":{\"type\":\"string\"},\"model\":{\"type\":\"string\"}}}";
    }

    private static JsonDeserializer deserializerWithAllowlist(String allowlist) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        configs.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED, "true");
        configs.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST, allowlist);
        return new JsonDeserializer(new GlueSchemaRegistryConfiguration(configs));
    }

    @Test
    public void testDeserialize_nullArgs_throwsException() {
        Schema testSchema = new Schema(GEOLOCATION_SCHEMA, DataFormat.JSON.name(), "testJson");
        byte[] testBytes = GEOLOCATION_PAYLOAD.getBytes(StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, () -> jsonDeserializer.deserialize(null, testSchema));
        assertThrows(IllegalArgumentException.class, () -> jsonDeserializer.deserialize(ByteBuffer.wrap(testBytes),
                                                                                        null));
    }

    @Test
    public void testDeserialize_schemaWithoutClassName_returnsJsonDataWithSchema() {
        Schema schema = new Schema(GEOLOCATION_SCHEMA, DataFormat.JSON.name(), "testJson");

        Object result = jsonDeserializer.deserialize(toSerializedBuffer(GEOLOCATION_PAYLOAD), schema);

        assertTrue(result instanceof JsonDataWithSchema);
    }

    @Test
    public void testDeserialize_schemaWithClassName_defaultConfig_returnsJsonDataWithSchema() {
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        // No config supplied (null) -> class name resolution defaults to disabled (secure default),
        // so the schema's className is ignored and a generic JsonDataWithSchema is returned.
        Object result = jsonDeserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof JsonDataWithSchema);
        assertEquals(CAR_PAYLOAD, ((JsonDataWithSchema) result).getPayload());
    }

    @Test
    public void testDeserialize_schemaWithClassName_resolutionEnabled_returnsSpecificPojo() {
        // Resolution enabled + className in allowlist -> typed POJO
        JsonDeserializer deserializer = deserializerWithAllowlist(
                "com.amazonaws.services.schemaregistry.serializers.json.Car");
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof Car);
    }

    @Test
    public void testDeserialize_schemaWithClassName_resolutionEnabled_noAllowlist_returnsJsonDataWithSchema() {
        // Resolution enabled but no allowlist configured -> falls back to JsonDataWithSchema
        JsonDeserializer deserializer = deserializerWithClassNameResolution(true);
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof JsonDataWithSchema);
        assertEquals(CAR_PAYLOAD, ((JsonDataWithSchema) result).getPayload());
    }

    @Test
    public void testDeserialize_schemaWithClassName_classNotInAllowlist_returnsJsonDataWithSchema() {
        // Resolution enabled + allowlist configured but className NOT in it -> falls back
        JsonDeserializer deserializer = deserializerWithAllowlist("com.example.SomeOtherClass");
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof JsonDataWithSchema);
        assertEquals(CAR_PAYLOAD, ((JsonDataWithSchema) result).getPayload());
    }

    @Test
    public void testDeserialize_schemaWithClassName_multipleClassesInAllowlist_returnsSpecificPojo() {
        // Multiple classes in allowlist, target is one of them
        JsonDeserializer deserializer = deserializerWithAllowlist(
                "com.example.Foo, com.amazonaws.services.schemaregistry.serializers.json.Car, com.example.Bar");
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof Car);
    }

    @Test
    public void testDeserialize_classNotInAllowlist_repeatedRecords_allReturnJsonDataWithSchema() {
        // The allowlist-miss warning is only logged once per class name; verify that suppressing
        // the log on later records does not change what those records deserialize to.
        JsonDeserializer deserializer = deserializerWithAllowlist("com.example.SomeOtherClass");
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        for (int i = 0; i < 3; i++) {
            Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

            assertTrue(result instanceof JsonDataWithSchema, "record " + i + " should not resolve to a POJO");
            assertEquals(CAR_PAYLOAD, ((JsonDataWithSchema) result).getPayload());
        }
    }

    @Test
    public void testDeserialize_schemaWithClassName_resolutionDisabled_returnsJsonDataWithSchema() {
        JsonDeserializer deserializer = deserializerWithClassNameResolution(false);
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        // Customer opted out of className resolution -> generic JsonDataWithSchema even though
        // the schema carries a className.
        Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof JsonDataWithSchema);
        assertEquals(CAR_PAYLOAD, ((JsonDataWithSchema) result).getPayload());
    }

    @Test
    public void testDeserialize_classNameInAllowedPackage_returnsSpecificPojo() {
        // A package entry spares consumers from listing every POJO on the topic.
        JsonDeserializer deserializer =
                deserializerWithAllowlist("com.amazonaws.services.schemaregistry.serializers.json.*");
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof Car);
    }

    @Test
    public void testDeserialize_classNameOutsideAllowedPackage_returnsJsonDataWithSchema() {
        // The allowed package is a sibling of the one the schema names, so it must not match.
        JsonDeserializer deserializer = deserializerWithAllowlist("com.amazonaws.services.schemaregistry.other.*");
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof JsonDataWithSchema);
        assertEquals(CAR_PAYLOAD, ((JsonDataWithSchema) result).getPayload());
    }

    @Test
    public void testDeserialize_manyDistinctDisallowedClassNames_warnStateStaysBounded() {
        // The warning dedup key is the schema's className, which the producer controls. Feeding a
        // stream of distinct disallowed names must not grow the dedup set without bound.
        JsonDeserializer deserializer = deserializerWithAllowlist("com.example.SomeOtherClass");

        for (int i = 0; i < 500; i++) {
            Schema schema = new Schema(schemaWithClassName("com.example.Generated" + i),
                                       DataFormat.JSON.name(), "testJson");

            Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

            assertTrue(result instanceof JsonDataWithSchema, "record " + i + " should not resolve to a POJO");
        }

        assertTrue(deserializer.getWarnedClassNames().size() <= MAX_WARNED_CLASS_NAMES,
                   "warned class name set grew past the cap: " + deserializer.getWarnedClassNames().size());
    }

    @Test
    public void testDeserialize_belowWarnCap_doesNotAnnounceSuppression() {
        // Suppression must not kick in for a class count a real consumer could reach.
        JsonDeserializer deserializer = deserializerWithAllowlist("com.example.SomeOtherClass");

        for (int i = 0; i < MAX_WARNED_CLASS_NAMES - 1; i++) {
            Schema schema = new Schema(schemaWithClassName("com.example.Generated" + i),
                                       DataFormat.JSON.name(), "testJson");
            deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);
        }

        assertEquals(MAX_WARNED_CLASS_NAMES - 1, deserializer.getWarnedClassNames().size());
        assertFalse(deserializer.getWarnCapNoticeEmitted().get(),
                    "suppression was announced before the cap was reached");
    }

    @Test
    public void testDeserialize_pastWarnCap_announcesSuppressionOnceAndStopsCollecting() {
        // Past the cap the deserializer stops warning rather than warning per record, and says so
        // once so that the silence is not itself a surprise.
        JsonDeserializer deserializer = deserializerWithAllowlist("com.example.SomeOtherClass");

        for (int i = 0; i < MAX_WARNED_CLASS_NAMES + 50; i++) {
            Schema schema = new Schema(schemaWithClassName("com.example.Generated" + i),
                                       DataFormat.JSON.name(), "testJson");
            deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);
        }

        // Single-threaded, so the cap is exact: names seen after it are not retained at all.
        assertEquals(MAX_WARNED_CLASS_NAMES, deserializer.getWarnedClassNames().size());
        assertFalse(deserializer.getWarnedClassNames().contains("com.example.Generated" + MAX_WARNED_CLASS_NAMES),
                    "a class name seen past the cap was still added to the dedup set");
        assertTrue(deserializer.getWarnCapNoticeEmitted().get(),
                   "reaching the cap did not announce that warnings are suppressed");
    }

    @Test
    public void testDeserialize_classNameInSubPackageOfAllowedPackage_returnsJsonDataWithSchema() {
        // The allowed package is the parent of the one the schema names. Allowing a package is not
        // a decision to allow everything beneath it, so this must not resolve.
        JsonDeserializer deserializer =
                deserializerWithAllowlist("com.amazonaws.services.schemaregistry.serializers.*");
        Schema schema = new Schema(CAR_SCHEMA, DataFormat.JSON.name(), "testJson");

        Object result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema);

        assertTrue(result instanceof JsonDataWithSchema);
        assertEquals(CAR_PAYLOAD, ((JsonDataWithSchema) result).getPayload());
    }
}
