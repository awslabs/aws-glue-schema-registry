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

package com.amazonaws.services.schemaregistry.caching;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaTest {

    @Test
    public void test_equality_Positive() throws Exception {

        String schemaToTest = "{\"type\":\"record\",\"name\":\"User3\",\"namespace\":\"com.amazonaws.services.schemaregistry.serializers.avro\",\"doc\":\"This schema is created for testing purpose\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"null\",\"int\"]},{\"name\":\"meta\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},{\"name\":\"listOfColours\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"integerEnum\",\"type\":{\"type\":\"enum\",\"name\":\"integerEnums\",\"symbols\":[\"ONE\",\"TWO\",\"THREE\",\"FOUR\"]}}]}";

        Schema objToTest = new Schema(schemaToTest, DataFormat.AVRO.name(),
                "test-schema");
        String fileName = "src/test/java/resources/avro/user3.avsc";
        org.apache.avro.Schema schema = getSchema(fileName);

        GenericData.EnumSymbol k = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> al = new ArrayList<>();
        al.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);

        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put("meta", map);
        genericRecordWithAllTypes.put("listOfColours", al);
        genericRecordWithAllTypes.put("integerEnum", k);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes);

        Schema obj = new Schema(schemaDefinition, DataFormat.AVRO.name(),
                "test-schema");

        assertTrue(EqualsBuilder.reflectionEquals(objToTest, obj));

    }

    @Test
    public void test_inequality_Positive_1() throws Exception {

        String schemaToTest = "{\"type\":\"record1\",\"name\":\"User3\",\"namespace\":\"com.amazonaws.services.schemaregistry.serializers.avro\",\"doc\":\"This schema is created for testing purpose\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"null\",\"int\"]},{\"name\":\"meta\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},{\"name\":\"listOfColours\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"integerEnum\",\"type\":{\"type\":\"enum\",\"name\":\"integerEnums\",\"symbols\":[\"ONE\",\"TWO\",\"THREE\",\"FOUR\"]}}]}";

        Schema objToTest = new Schema(schemaToTest, DataFormat.AVRO.name(),
                "test-schema");
        String fileName = "src/test/java/resources/avro/user3.avsc";
        org.apache.avro.Schema schema = getSchema(fileName);

        GenericData.EnumSymbol k = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> al = new ArrayList<>();
        al.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);

        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put("meta", map);
        genericRecordWithAllTypes.put("listOfColours", al);
        genericRecordWithAllTypes.put("integerEnum", k);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes);

        Schema obj = new Schema(schemaDefinition, DataFormat.AVRO.name(),
                "test-schema");

        assertFalse(EqualsBuilder.reflectionEquals(objToTest, obj));

    }

    @Test
    public void test_inequality_Positive_2() throws Exception {

        String schemaToTest = "{\"type\":\"record\",\"name\":\"User3\",\"namespace\":\"com.amazonaws.services.schemaregistry.serializers.avro\",\"doc\":\"This schema is created for testing purpose\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"null\",\"int\"]},{\"name\":\"meta\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},{\"name\":\"listOfColours\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"integerEnum\",\"type\":{\"type\":\"enum\",\"name\":\"integerEnums\",\"symbols\":[\"ONE\",\"TWO\",\"THREE\",\"FOUR\"]}}]}";

        Schema objToTest = new Schema(schemaToTest, "PROTOBUFF",
                "test-schema");
        String fileName = "src/test/java/resources/avro/user3.avsc";
        org.apache.avro.Schema schema = getSchema(fileName);

        GenericData.EnumSymbol k = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> al = new ArrayList<>();
        al.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);

        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put("meta", map);
        genericRecordWithAllTypes.put("listOfColours", al);
        genericRecordWithAllTypes.put("integerEnum", k);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes);

        Schema obj = new Schema(schemaDefinition, DataFormat.AVRO.name(),
                "test-schema");

        assertFalse(EqualsBuilder.reflectionEquals(objToTest, obj));

    }

    private org.apache.avro.Schema getSchema(String fileName) throws IOException {
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();

        return parser.parse(new File(fileName));
    }

}
