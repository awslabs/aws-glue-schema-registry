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
package com.amazonaws.services.schemaregistry.integrationtests.generators;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Avro Generic Record generator with Backward compatibility
 */
public class AvroGenericBackwardCompatDataGenerator implements TestDataGenerator<GenericRecord> {
    @Override
    public List<GenericRecord> createRecords() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema1 = null;
        Schema schema2 = null;
        try {
            schema1 = parser.parse(new File("src/test/resources/avro/backward/backward1.avsc"));
            schema2 = parser.parse(new File("src/test/resources/avro/backward/backward2.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenericRecord backward11 = new GenericData.Record(schema1);
        backward11.put("id", "11");
        backward11.put("f1", "curfew");

        GenericRecord backward12 = new GenericData.Record(schema1);
        backward12.put("id", "12");
        backward12.put("f1", "covid-19");

        GenericRecord backward21 = new GenericData.Record(schema2);
        backward21.put("id", "21");
        backward21.put("f1", "happy");
        backward21.put("f2", "birthday");

        GenericRecord backward22 = new GenericData.Record(schema2);
        backward22.put("id", "22");
        backward22.put("f1", "merry");
        backward22.put("f2", "christmas");

        GenericRecord backward13 = new GenericData.Record(schema1);
        backward13.put("id", "13");
        backward13.put("f1", "social distancing");

        GenericRecord backward23 = new GenericData.Record(schema2);
        backward23.put("id", "23");
        backward23.put("f1", "good");
        backward23.put("f2", "morning");

        List<GenericRecord> data = new ArrayList<>();
        data.add(backward11);
        data.add(backward12);
        data.add(backward21);
        data.add(backward22);
        data.add(backward13);
        data.add(backward23);

        return data;
    }
}
