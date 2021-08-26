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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import com.amazonaws.services.schemaregistry.common.Schema;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonDeserializerTest {
    private JsonDeserializer jsonDeserializer = new JsonDeserializer(null);

    @Test
    public void testDeserialize_nullArgs_throwsException() {
        String testSchemaDefinition = "{\"$id\":\"https://example.com/geographical-location.schema.json\","
                                      + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Longitude "
                                      + "and Latitude Values\",\"description\":\"A geographical coordinate.\","
                                      + "\"required\":[\"latitude\",\"longitude\"],\"type\":\"object\","
                                      + "\"properties\":{\"latitude\":{\"type\":\"number\",\"minimum\":-90,"
                                      + "\"maximum\":90},\"longitude\":{\"type\":\"number\",\"minimum\":-180,"
                                      + "\"maximum\":180}},\"additionalProperties\":false}";
        String jsonData = "{\"latitude\":48.858093,\"longitude\":2.294694}";
        byte[] testBytes = jsonData.getBytes(StandardCharsets.UTF_8);

        Schema testSchema = new Schema(testSchemaDefinition, DataFormat.JSON.name(), "testJson");

        assertThrows(IllegalArgumentException.class, () -> jsonDeserializer.deserialize(null, testSchema));
        assertThrows(IllegalArgumentException.class, () -> jsonDeserializer.deserialize(ByteBuffer.wrap(testBytes),
                                                                                        null));
    }
}
