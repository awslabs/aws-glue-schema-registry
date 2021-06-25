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

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Avro Generic Record generator with Backward All compatibility
 */
@Slf4j
public class AvroGenericBackwardAllCompatDataGenerator implements TestDataGenerator<GenericRecord> {
    @Override
    public List<GenericRecord> createRecords() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema1 = null;
        Schema schema2 = null;
        Schema schema3 = null;
        try {
            schema1 = parser.parse(new File("src/test/resources/avro/backwardAll/backwardAll1.avsc"));
            schema2 = parser.parse(new File("src/test/resources/backwardAll/backwardAll2.avsc"));
            schema3 = parser.parse(new File("src/test/resources/backwardAll/backwardAll3.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenericRecord backwardAll11 = new GenericData.Record(schema1);
        backwardAll11.put("f1", "game");
        backwardAll11.put("f2", "station");
        backwardAll11.put("f3", 0);

        GenericRecord backwardAll21 = new GenericData.Record(schema2);
        backwardAll21.put("f1", "disney");
        backwardAll21.put("f2", "plus");

        GenericRecord backwardAll31 = new GenericData.Record(schema3);
        backwardAll31.put("f1", "ladies");

        GenericRecord backwardAll12 = new GenericData.Record(schema1);
        backwardAll12.put("f1", "tamper");
        backwardAll12.put("f2", "monkey");
        backwardAll12.put("f3", 1);

        GenericRecord backwardAll22 = new GenericData.Record(schema2);
        backwardAll22.put("f1", "hbo");
        backwardAll22.put("f2", "max");

        GenericRecord backwardAll32 = new GenericData.Record(schema3);
        backwardAll32.put("f1", "gentlemen");

        List<GenericRecord> data = new ArrayList<>();
        data.add(backwardAll11);
        data.add(backwardAll21);
        data.add(backwardAll31);
        data.add(backwardAll12);
        data.add(backwardAll22);
        data.add(backwardAll32);

        return data;
    }
}
