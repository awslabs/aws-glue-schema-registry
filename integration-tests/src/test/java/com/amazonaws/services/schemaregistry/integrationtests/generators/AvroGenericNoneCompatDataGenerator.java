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
import java.util.ArrayList;
import java.util.List;

/**
 * Avro Generic Record generator with None compatibility
 */
public class AvroGenericNoneCompatDataGenerator implements TestDataGenerator<GenericRecord> {
    @Override
    public List<GenericRecord> createRecords() throws Exception {
        Schema.Parser parser = new Schema.Parser();

        Schema schemaUser = parser.parse(new File("src/test/resources/avro/user.avsc"));
        Schema schemaPayment = parser.parse(new File("src/test/resources/avro/Payment.avsc"));

        GenericRecord sansa = new GenericData.Record(schemaUser);
        sansa.put("name", "Sansa");
        sansa.put("favorite_number", 99);
        sansa.put("favorite_color", "white");

        GenericRecord harry = new GenericData.Record(schemaUser);
        harry.put("name", "Harry");
        harry.put("favorite_number", 10);
        harry.put("favorite_color", "black");

        GenericRecord hermione = new GenericData.Record(schemaUser);
        hermione.put("name", "Hermione");
        hermione.put("favorite_number", 1);
        hermione.put("favorite_color", "red");

        GenericRecord ron = new GenericData.Record(schemaUser);
        ron.put("name", "Ron");
        ron.put("favorite_number", 18);
        ron.put("favorite_color", "green");

        GenericRecord jay = new GenericData.Record(schemaUser);
        jay.put("name", "Jay");
        jay.put("favorite_number", 0);
        jay.put("favorite_color", "pink");

        GenericRecord grocery = new GenericData.Record(schemaPayment);
        grocery.put("id", "grocery_1");
        grocery.put("amount", 25.5);

        GenericRecord commute = new GenericData.Record(schemaPayment);
        commute.put("id", "commute_1");
        commute.put("amount", 3.5);

        GenericRecord movie = new GenericData.Record(schemaPayment);
        movie.put("id", "entertainment_1");
        movie.put("amount", 19.2);

        GenericRecord musical = new GenericData.Record(schemaPayment);
        musical.put("id", "entertainment_2");
        musical.put("amount", 105.0);

        GenericRecord parking = new GenericData.Record(schemaPayment);
        parking.put("id", "commute_2");
        parking.put("amount", 15.0);

        List<GenericRecord> records = new ArrayList<>();
        records.add(sansa);
        records.add(harry);
        records.add(hermione);
        records.add(ron);
        records.add(jay);
        records.add(grocery);
        records.add(commute);
        records.add(movie);
        records.add(musical);
        records.add(parking);

        return records;
    }
}
