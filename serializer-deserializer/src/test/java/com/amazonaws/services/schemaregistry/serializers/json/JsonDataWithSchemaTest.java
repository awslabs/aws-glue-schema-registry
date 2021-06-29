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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonDataWithSchemaTest {
    @Test
    public void testInValidJsonDataWithSchema() {
        assertThrows(IllegalArgumentException.class, () -> JsonDataWithSchema.builder("", "")
                .build());
        assertThrows(IllegalArgumentException.class, () -> JsonDataWithSchema.builder(null, "")
                .build());
        assertThrows(IllegalArgumentException.class, () -> JsonDataWithSchema.builder("", null)
                .build());
        assertThrows(IllegalArgumentException.class, () -> JsonDataWithSchema.builder(null, null)
                .build());
    }

    @Test
    public void testEmptyValidJsonDataWithSchema() {
        String schema = "{\n" + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"$id\": \"http://example.com/product.schema.json\",\n" + "  \"title\": \"Product\",\n"
                        + "  \"description\": \"A product in the catalog\",\n" + "  \"type\": \"string\"\n" + "}";
        String payload = "";
        JsonDataWithSchema jsonDataWithSchema = JsonDataWithSchema.builder(schema, payload)
                .build();
        assertNotNull(jsonDataWithSchema);
    }

    @Test
    public void testNullJsonDataWithSchema() {
        String schema = "{\n" + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"$id\": \"http://example.com/product.schema.json\",\n" + "  \"title\": \"Product\",\n"
                        + "  \"description\": \"A product in the catalog\",\n" + "  \"type\": \"null\"\n" + "}";
        String payload = null;
        JsonDataWithSchema jsonDataWithSchema = JsonDataWithSchema.builder(schema, payload)
                .build();
        assertNotNull(jsonDataWithSchema);
    }
}
