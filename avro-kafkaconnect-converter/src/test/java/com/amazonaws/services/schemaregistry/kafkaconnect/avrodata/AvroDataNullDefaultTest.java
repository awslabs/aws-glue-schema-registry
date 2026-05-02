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

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AvroDataNullDefaultTest {

    private AvroData avroData;

    @BeforeEach
    void setUp() {
        avroData = new AvroData(2);
    }

    @Test
    void testToConnectDataWithNullDefaultInRecord() {
        // Schema with a record field that has a default containing a null value
        // This is the exact scenario from issue #307
        String schemaJson = "{"
                + "  \"type\": \"record\","
                + "  \"name\": \"Product\","
                + "  \"fields\": [{"
                + "    \"name\": \"name\","
                + "    \"type\": {"
                + "      \"type\": \"record\","
                + "      \"name\": \"LanguageString\","
                + "      \"fields\": [{"
                + "        \"name\": \"de\","
                + "        \"type\": \"string\""
                + "      }, {"
                + "        \"name\": \"en\","
                + "        \"type\": [\"null\", \"string\"]"
                + "      }]"
                + "    },"
                + "    \"default\": {\"de\": \"\", \"en\": null}"
                + "  }]"
                + "}";

        Schema avroSchema = new Schema.Parser().parse(schemaJson);

        Schema langStringSchema = avroSchema.getField("name").schema();
        GenericData.Record langString = new GenericRecordBuilder(langStringSchema)
                .set("de", "Hallo")
                .set("en", null)
                .build();
        GenericData.Record product = new GenericRecordBuilder(avroSchema)
                .set("name", langString)
                .build();

        SchemaAndValue result = avroData.toConnectData(avroSchema, product);

        assertNotNull(result);
        assertNotNull(result.schema());
        Struct productStruct = (Struct) result.value();
        Struct nameStruct = (Struct) productStruct.get("name");
        assertEquals("Hallo", nameStruct.get("de"));
        assertNull(nameStruct.get("en"));
    }
}
