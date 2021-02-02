package com.amazonaws.services.schemaregistry.integration_tests.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility functions class for the executing the tests
 */
@Slf4j
public class Utils {

    public static List<GenericRecord> createGenericAvroRecords() throws Exception {
        log.info("Start creating Avro records for testing ...");
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

        log.info("Finish creating Avro records.");
        return records;

    }

    public static List<Person> createSpecificAvroRecords() {
        log.info("Start creating Specific Avro records for testing ...");

        Person jane = Person.newBuilder()
                .setAge(18)
                .setFirstName("Jane")
                .setLastName("Doe")
                .setHeight(178f)
                .setEmployed(true)
                .build();

        Person john = Person.newBuilder()
                .setAge(28)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(188f)
                .setEmployed(false)
                .build();

        List<Person> records = new ArrayList<>();
        records.add(jane);
        records.add(john);

        log.info("Finished creating specific Avro records.");
        return records;
    }

    public static List<GenericRecord> generateRecordsForBackwardCompatibility() {
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

        List<GenericRecord> data = new ArrayList<GenericRecord>();
        data.add(backward11);
        data.add(backward12);
        data.add(backward21);
        data.add(backward22);
        data.add(backward13);
        data.add(backward23);

        return data;
    }

    public static List<GenericRecord> generateRecordsForBackwardAllCompatibility() {
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

        List<GenericRecord> data = new ArrayList<GenericRecord>();
        data.add(backwardAll11);
        data.add(backwardAll21);
        data.add(backwardAll31);
        data.add(backwardAll12);
        data.add(backwardAll22);
        data.add(backwardAll32);

        return data;
    }

}
