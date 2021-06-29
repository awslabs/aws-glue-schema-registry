package com.amazonaws.services.schemaregistry.kafkastreams.utils;

import com.amazonaws.services.schemaregistry.kafkastreams.utils.avro.User;
import com.amazonaws.services.schemaregistry.kafkastreams.utils.json.Car;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.fail;

public final class RecordGenerator {

    public static final String AVRO_USER_SCHEMA_FILE_PATH = "src/test/resources/avro/user.avsc";
    public static final String JSON_PERSON_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/person.schema.json";
    public static final String JSON_PERSON_DATA_FILE_PATH = "src/test/resources/json/person1.json";
    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Loads and Parses schema from file.
     *
     * @param schemaFilePath Schema string
     * @return Avro schema object
     */
    public static Schema loadAvroSchema(String schemaFilePath) {
        Schema schema = null;
        try {
            schema = (new Schema.Parser()).parse(new File(schemaFilePath));
        } catch (Exception e) {
            fail("Failed to parse the avro schema file", e);
        }
        return schema;
    }

    /**
     * Loads and Parses schema from file.
     *
     * @param jsonFilePath Schema string
     * @return JSON string
     */
    public static String loadJson(String jsonFilePath) {
        String json = null;
        try {
            File file = new File(jsonFilePath);
            json = objectMapper.readTree(file).toString();
        } catch (Exception e) {
            fail(String.format("Failed to load the json file : ", jsonFilePath), e);
        }
        return json;
    }

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericRecord createGenericAvroRecord() {
        Schema schema = loadAvroSchema(AVRO_USER_SCHEMA_FILE_PATH);
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", "sansa");
        genericRecord.put("favorite_number", 99);
        genericRecord.put("favorite_color", "red");

        return genericRecord;
    }

    /**
     * Helper method to create a test user object
     *
     * @return constructed user object instance
     */
    public static User createSpecificAvroRecord() {
        return User.newBuilder()
                .setName("test")
                .setFavoriteColor("violet")
                .setFavoriteNumber(10)
                .build();
    }

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return JsonDataWithSchema
     */
    public static JsonDataWithSchema createGenericJsonRecord() {
        String schema = loadJson(JSON_PERSON_SCHEMA_FILE_PATH);
        String data = loadJson(JSON_PERSON_DATA_FILE_PATH);

        return JsonDataWithSchema.builder(schema, data)
                .build();
    }

    /**
     * Helper method to create a test specific record of type Car
     *
     * @return constructed user object instance
     */
    public static Car createSpecificJsonRecord() {
        GregorianCalendar calendar = new GregorianCalendar(2014, Calendar.FEBRUARY, 11);
        calendar.setTimeZone(TimeZone.getTimeZone("PST"));

        return Car.builder()
                .make("Honda")
                .model("crv")
                .used(true)
                .miles(10000)
                .year(2016)
                .listedDate(calendar.getTime())
                .purchaseDate(Date.from(Instant.parse("2000-01-01T00:00:00.000Z")))
                .owners(new String[]{"John", "Jane", "Hu"})
                .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
                .build();
    }
}
