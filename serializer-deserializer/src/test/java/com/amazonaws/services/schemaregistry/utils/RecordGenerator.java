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
package com.amazonaws.services.schemaregistry.utils;

import com.amazonaws.services.schemaregistry.serializers.avro.User;
import com.amazonaws.services.schemaregistry.serializers.json.Car;
import com.amazonaws.services.schemaregistry.serializers.json.Employee;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public final class RecordGenerator {
    public static final String AVRO_USER_SCHEMA_FILE_PATH = "src/test/resources/avro/user.avsc";
    public static final String AVRO_EMP_RECORD_SCHEMA_FILE_PATH = "src/test/resources/avro/emp_record.avsc";
    public static final String AVRO_USER_ENUM_SCHEMA_FILE = "src/test/resources/avro/user_enum.avsc";
    public static final String AVRO_USER_ARRAY_SCHEMA_FILE = "src/test/resources/avro/user_array.avsc";
    public static final String AVRO_USER_UNION_SCHEMA_FILE = "src/test/resources/avro/user_union.avsc";
    public static final String AVRO_USER_FIXED_SCHEMA_FILE = "src/test/resources/avro/user_fixed.avsc";
    public static final String AVRO_USER_ARRAY_STRING_SCHEMA_FILE = "src/test/resources/avro/user_array_String.avsc";
    public static final String AVRO_USER_MAP_SCHEMA_FILE = "src/test/resources/avro/user_map.avsc";
    public static final String AVRO_USER_MIXED_TYPE_SCHEMA_FILE = "src/test/resources/avro/user3.avsc";
    public static final String AVRO_USER_LOGICAL_TYPES_SCHEMA_FILE_PATH = "src/test/resources/avro/user4.avsc";
    public static final String JSON_PERSON_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/person.schema.json";
    public static final String JSON_PERSON_DATA_FILE_PATH = "src/test/resources/json/person1.json";
    public static final String JSON_PRODUCE_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/produce_ref.schema.json";
    public static final String JSON_PRODUCE_DATA_FILE_PATH = "src/test/resources/json/produce1.json";
    public static final String JSON_GEOLOCATION_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/geographical-location.schema.json";
    public static final String JSON_GEOLOCATION_DATA_FILE_PATH = "src/test/resources/json/geolocation1.json";
    public static final String JSON_NULL_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/null.schema.json";
    public static final String JSON_NULL_DATA_FILE_PATH = "src/test/resources/json/null.json";
    public static final String JSON_STRING_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/string.schema.json";
    public static final String JSON_STRING_DATA_FILE_PATH = "src/test/resources/json/string.json";
    public static final String JSON_EMPTY_STRING_DATA_FILE_PATH = "src/test/resources/json/empty.json";
    public static final String JSON_NUMBER_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/number.schema.json";
    public static final String JSON_INTEGER_DATA_FILE_PATH = "src/test/resources/json/integer.json";
    public static final String JSON_FLOAT_DATA_FILE_PATH = "src/test/resources/json/float.json";
    public static final String JSON_BIG_INTEGER_DATA_FILE_PATH = "src/test/resources/json/bigint.json";
    public static final String JSON_BIG_DECIMAL_DATA_FILE_PATH = "src/test/resources/json/bigdecimal.json";
    public static final String JSON_INVALID_PRODUCE_1_DATA_FILE_PATH = "src/test/resources/json/invalidProduce1.json";
    public static final String JSON_INVALID_PRODUCE_2_DATA_FILE_PATH = "src/test/resources/json/invalidProduce2.json";
    public static final String JSON_DATE_TIME_ARRAY_DATA_FILE_PATH = "src/test/resources/json/dateTimeArray.json";
    public static final String JSON_DATE_TIME_ARRAY_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/dateTimeArray.schema.json";
    public static final String JSON_PRODUCT_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/product.schema.json";
    public static final String JSON_PRODUCT_URL_REF_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/productURLRef.schema.json";
    public static final String JSON_PRODUCT_INVALID_URL_REF_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/productInvalidURLRef.schema.json";
    public static final String JSON_PRODUCT_DATA_FILE_PATH = "src/test/resources/json/product.json";
    public static final String JSON_PERSON_RECURSIVE_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/personRecursive.schema.json";
    public static final String JSON_PERSON_RECURSIVE_DATA_FILE_PATH = "src/test/resources/json/personRecursive.json";
    public static final String JSON_ADDRESS_EXTENDED_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft06/addressExtended.schema.json";
    public static final String JSON_ADDRESS1_DATA_FILE_PATH = "src/test/resources/json/address1.json";
    public static final String JSON_ADDRESS2_DATA_FILE_PATH = "src/test/resources/json/address2.json";
    public static final String JSON_ADDRESS_REF_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/addressRef.schema.json";
    public static final String JSON_ADDRESS_ID_REF_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/addressIdRef.schema.json";
    public static final String JSON_ADDRESS3_DATA_FILE_PATH = "src/test/resources/json/address3.json";
    public static final String JSON_ADDRESS_IF_ELSE_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/addressIfThenElse.schema.json";
    public static final String JSON_ADDRESS_USA_DATA_FILE_PATH = "src/test/resources/json/addressUSA.json";
    public static final String JSON_ADDRESS_CA_DATA_FILE_PATH = "src/test/resources/json/addressCA.json";
    public static final String JSON_ADDRESS_CA_INVALID_DATA_FILE_PATH = "src/test/resources/json/addressCAInvalid.json";
    public static final String JSON_ADDRESS_HTML_ENCODING_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/contentEncodingHtml.schema.json";
    public static final String JSON_ADDRESS_HTML_ENCODING_DATA_FILE_PATH = "src/test/resources/json/html.json";
    public static final String JSON_ADDRESS_BASE64_ENCODING_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/contentEncodingBase64Image.schema.json";
    public static final String JSON_ADDRESS_BASE64_ENCODING_DATA_FILE_PATH =
            "src/test/resources/json/base64EncodedImage.json";
    public static final String JSON_CONSTANT_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft06/constant.schema.json";
    public static final String JSON_CONSTANT_DATA_FILE_PATH = "src/test/resources/json/constant.json";
    public static final String JSON_CONSTANT_INVALID_DATA_FILE_PATH = "src/test/resources/json/constantInvalid.json";
    public static final String JSON_EMPLOYEE_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft04/employee.schema.json";
    public static final String JSON_EMPLOYEE_DATA_FILE_PATH = "src/test/resources/json/employee.json";
    public static final String JSON_WITHOUT_SPEC_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft04/withoutSchemaSpec.schema.json";
    public static final String JSON_NULL_SPEC_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft04/withoutSchemaSpec.schema.json";
    public static final String JSON_DATES_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/dates.schema.json";
    public static final String JSON_DATES_FILE_PATH = "src/test/resources/json/dates.json";
    public static final String JSON_BOOLEAN_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/boolean.schema.json";
    public static final String JSON_BOOLEAN_FILE_PATH = "src/test/resources/json/boolean.json";
    public static final String JSON_DDB_SCHEMA_FILE_PATH =
            "src/test/resources/json/schema/draft07/ddb.schema.json";
    public static final String JSON_DDB_FILE_PATH = "src/test/resources/json/ddb.json";

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericRecord createGenericAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE_PATH);
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", "sansa");
        genericRecord.put("favorite_number", 99);
        genericRecord.put("favorite_color", "red");

        return genericRecord;
    }

    /**
     * Test Helper method to generate a test ENUM GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.EnumSymbol createGenericUserEnumAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_ENUM_SCHEMA_FILE);
        GenericData.EnumSymbol genericRecord = new GenericData.EnumSymbol(schema, "ONE");

        return genericRecord;
    }

    /**
     * Test Helper method to generate an invalid test Avro ENUM GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.EnumSymbol createGenericUserInvalidEnumAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_ENUM_SCHEMA_FILE);
        GenericData.EnumSymbol genericRecord = new GenericData.EnumSymbol(schema, "SPADE");

        return genericRecord;
    }

    /**
     * Test Helper method to generate a test Avro integer Array GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Array<Integer> createGenericIntArrayAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_SCHEMA_FILE);
        GenericData.Array<Integer> array = new GenericData.Array<>(1, schema);
        array.add(1);

        return array;
    }

    /**
     * Test Helper method to generate a test Avro String Array GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Array<String> createGenericStringArrayAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_STRING_SCHEMA_FILE);
        GenericData.Array<String> array = new GenericData.Array<>(1, schema);
        array.add("2");

        return array;
    }

    /**
     * Test Helper method to generate a test Avro invalid Array GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Array<Object> createGenericUserInvalidArrayAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_SCHEMA_FILE);
        GenericData.Array<Object> array = new GenericData.Array<>(1, schema);
        array.add("s");

        return array;
    }

    /**
     * Test Helper method to generate a test Avro Map GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Record createGenericUserMapAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_MAP_SCHEMA_FILE);
        GenericData.Record mapRecord = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);
        mapRecord.put("meta", map);

        return mapRecord;
    }

    /**
     * Test Helper method to generate a test invalid Avro Map GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Record createGenericInvalidMapAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_MAP_SCHEMA_FILE);
        GenericData.Record mapRecord = new GenericData.Record(schema);
        Map<String, Object> map = new HashMap<>();
        map.put("test", "s");
        mapRecord.put("meta", map);

        return mapRecord;
    }

    /**
     * Test Helper method to generate a test Avro Union GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Record createGenericUserUnionAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE);
        GenericData.Record unionRecord = new GenericData.Record(schema);
        unionRecord.put("experience", 1);
        unionRecord.put("age", 30);

        return unionRecord;
    }

    /**
     * Test Helper method to generate a test Avro Union GenericRecord with null
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Record createGenericUnionWithNullValueAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE);
        GenericData.Record unionRecord = new GenericData.Record(schema);
        unionRecord.put("experience", null);
        unionRecord.put("age", 30);

        return unionRecord;
    }

    /**
     * Test Helper method to generate a test Avro invalid Union GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Record createGenericInvalidUnionAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE);
        GenericData.Record unionRecord = new GenericData.Record(schema);
        unionRecord.put("experience", "wrong_value");
        unionRecord.put("age", 30);

        return unionRecord;
    }

    /**
     * Test Helper method to generate a test Avro Fixed GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Fixed createGenericFixedAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_FIXED_SCHEMA_FILE);
        GenericData.Fixed fixedRecord = new GenericData.Fixed(schema);
        byte[] bytes = "byte array".getBytes();
        fixedRecord.bytes(bytes);

        return fixedRecord;
    }

    /**
     * Test Helper method to generate a test Avro Invalid Fixed GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Fixed createGenericInvalidFixedAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_FIXED_SCHEMA_FILE);
        GenericData.Fixed fixedRecord = new GenericData.Fixed(schema);
        byte[] bytes = "byte".getBytes();
        fixedRecord.bytes(bytes);

        return fixedRecord;
    }

    /**
     * Test Helper method to generate a test Avro mized types GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericData.Record createGenericMultipleTypesAvroRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE);

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

        return genericRecordWithAllTypes;
    }

    /**
     * Test Helper method to generate a test GenericRecord with logical types
     *
     * @return Generic AVRO Record
     */
    public static GenericRecord createGenericAvroRecordWithLogicalTypes() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_USER_LOGICAL_TYPES_SCHEMA_FILE_PATH);
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", "Sylvestre");
        genericRecord.put("dateOfBirth", LocalDate.parse("2021-05-01"));
        genericRecord.put("age", new BigDecimal("1.56"));

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
     * Helper method to generate a test GenericRecord from emp record schema
     *
     * @return Generic AVRO Record
     */
    public static GenericRecord createGenericEmpRecord() {
        Schema schema = SchemaLoader.loadAvroSchema(AVRO_EMP_RECORD_SCHEMA_FILE_PATH);
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", "xyz");
        genericRecord.put("id", 001);
        genericRecord.put("salary", 30000);
        genericRecord.put("age", 25);
        genericRecord.put("address", "abc");
        return genericRecord;
    }

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return JsonDataWithSchema
     */
    public static JsonDataWithSchema createGenericJsonRecord(TestJsonRecord testJsonRecord) {
        String schema = SchemaLoader.loadJson(testJsonRecord.getSchemaPath());
        String payload = SchemaLoader.loadJson(testJsonRecord.getDataPath());

        return JsonDataWithSchema.builder(schema, payload)
                .build();
    }

    /**
     * Test Helper method to generate a GenericRecord with invalid schema
     *
     * @return JsonDataWithSchema
     */
    public static JsonDataWithSchema createRecordWithMalformedJsonSchema() {
        String schema = "{\n" + "  \"$id\": \"https://example.com/string.schema.json\",\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"description\": \"String schema\",\n" + "  \"type\": \"string\",\n"
                        + "  \"additionalProperties\": false,\n" + "}";
        String payload = "abcd";

        return JsonDataWithSchema.builder(schema, payload)
                .build();
    }

    /**
     * Test Helper method to generate a GenericRecord with invalid schema
     *
     * @return JsonDataWithSchema
     */
    public static JsonDataWithSchema createRecordWithMalformedJsonData() {
        String schema = "{\n" + "  \"$id\": \"https://example.com/geographical-location.schema.json\",\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"title\": \"Longitude and Latitude Values\",\n"
                        + "  \"description\": \"A geographical coordinate.\",\n"
                        + "  \"required\": [ \"latitude\", \"longitude\" ],\n" + "  \"type\": \"object\",\n"
                        + "  \"properties\": {\n" + "    \"latitude\": {\n" + "      \"type\": \"number\",\n"
                        + "      \"minimum\": -90,\n" + "      \"maximum\": 90\n" + "    },\n"
                        + "    \"longitude\": {\n" + "      \"type\": \"number\",\n" + "      \"minimum\": -180,\n"
                        + "      \"maximum\": 180\n" + "    }\n" + "  },\n" + "  \"additionalProperties\": false\n"
                        + "}";
        String payload = "{\n" + "  \"latitude\": 48.858093,\n" + "  \"longitude\": 2.294694,\n" + "}";

        return JsonDataWithSchema.builder(schema, payload)
                .build();
    }

    public static JsonDataWithSchema createNonSchemaConformantJsonData() {
        TestJsonRecord invalidTestRecord = TestJsonRecord.valueOf(TestJsonRecord.INVALIDPERSON.name());
        return createGenericJsonRecord(invalidTestRecord);
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

    /**
     * Helper method to create a test invalid specific record of type Car
     *
     * @return constructed user object instance
     */
    public static Car createInvalidSpecificJsonRecord() {
        return Car.builder()
                .make("Honda")
                .model("crv")
                .used(true)
                .miles(300000)
                .year(1999)
                .owners(new String[]{"John", "Jane", "Hu"})
                .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
                .build();
    }

    /**
     * Helper method to create a test invalid specific record of type Employee
     * with wrong class name
     *
     * @return constructed user object instance
     */
    public static Employee createInvalidEmployeeJsonRecord() {
        return Employee.builder()
                .name("JohnDoe")
                .build();
    }

    /**
     * Helper method to create a test null specific record
     *
     * @return constructed user object instance
     */
    public static Object createNullSpecificJsonRecord() {
        return new Object();
    }

    public enum TestJsonRecord {
        PERSON(JSON_PERSON_SCHEMA_FILE_PATH, JSON_PERSON_DATA_FILE_PATH, true),
        PRODUCE(JSON_PRODUCE_SCHEMA_FILE_PATH, JSON_PRODUCE_DATA_FILE_PATH, true),
        GEOLOCATION(JSON_GEOLOCATION_SCHEMA_FILE_PATH, JSON_GEOLOCATION_DATA_FILE_PATH, true),
        NULLSTRING(JSON_NULL_SCHEMA_FILE_PATH, JSON_NULL_DATA_FILE_PATH, true),
        NONEMPTYSTRING(JSON_STRING_SCHEMA_FILE_PATH, JSON_STRING_DATA_FILE_PATH, true),
        EMPTYSTRING(JSON_STRING_SCHEMA_FILE_PATH, JSON_EMPTY_STRING_DATA_FILE_PATH, true),
        INTEGER(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_INTEGER_DATA_FILE_PATH, true),
        FLOAT(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_FLOAT_DATA_FILE_PATH, true),
        BIGINTEGER(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_BIG_INTEGER_DATA_FILE_PATH, true),
        BIGDECIMAL(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_BIG_DECIMAL_DATA_FILE_PATH, true),
        DATETIMEARRAY(JSON_DATE_TIME_ARRAY_SCHEMA_FILE_PATH, JSON_DATE_TIME_ARRAY_DATA_FILE_PATH, true),
        PERSONRECURSIVE(JSON_PERSON_RECURSIVE_SCHEMA_FILE_PATH, JSON_PERSON_RECURSIVE_DATA_FILE_PATH, true),
        PRODUCT_REMOTE_REF(JSON_PRODUCT_SCHEMA_FILE_PATH, JSON_PRODUCT_DATA_FILE_PATH, false), // true if local reference is allowed
        PRODUCT_INVALID_URL_REF(JSON_PRODUCT_INVALID_URL_REF_SCHEMA_FILE_PATH, JSON_PRODUCT_DATA_FILE_PATH, false),
        PRODUCT_URL_REF(JSON_PRODUCT_URL_REF_SCHEMA_FILE_PATH, JSON_PRODUCT_DATA_FILE_PATH, false), // true if remote reference is allowed
        ADDRESSREF(JSON_ADDRESS_REF_SCHEMA_FILE_PATH, JSON_ADDRESS3_DATA_FILE_PATH, true),
        ADDRESSIDREF(JSON_ADDRESS_ID_REF_SCHEMA_FILE_PATH, JSON_ADDRESS3_DATA_FILE_PATH, true),
        ADDRESSEXTENDED(JSON_ADDRESS_EXTENDED_SCHEMA_FILE_PATH, JSON_ADDRESS1_DATA_FILE_PATH, true),
        ADDRESSEXTENDEDINVALID(JSON_ADDRESS_EXTENDED_SCHEMA_FILE_PATH, JSON_ADDRESS2_DATA_FILE_PATH, false),
        ADDRESSUSA(JSON_ADDRESS_IF_ELSE_SCHEMA_FILE_PATH, JSON_ADDRESS_USA_DATA_FILE_PATH, true),
        ADDRESSCA(JSON_ADDRESS_IF_ELSE_SCHEMA_FILE_PATH, JSON_ADDRESS_CA_DATA_FILE_PATH, true),
        ADDRESSCAINVALID(JSON_ADDRESS_IF_ELSE_SCHEMA_FILE_PATH, JSON_ADDRESS_CA_INVALID_DATA_FILE_PATH, false),
        HTMLENCODED(JSON_ADDRESS_HTML_ENCODING_SCHEMA_FILE_PATH, JSON_ADDRESS_HTML_ENCODING_DATA_FILE_PATH, true),
        BASE64ENCODED(JSON_ADDRESS_BASE64_ENCODING_SCHEMA_FILE_PATH, JSON_ADDRESS_BASE64_ENCODING_DATA_FILE_PATH, true),
        CONSTANT(JSON_CONSTANT_SCHEMA_FILE_PATH, JSON_CONSTANT_DATA_FILE_PATH, true),
        CONSTANTINVALID(JSON_CONSTANT_SCHEMA_FILE_PATH, JSON_CONSTANT_INVALID_DATA_FILE_PATH, false),
        INVALIDPERSON(JSON_PERSON_SCHEMA_FILE_PATH, JSON_PRODUCE_DATA_FILE_PATH, false),
        INVALIDPRODUCE1(JSON_PRODUCE_SCHEMA_FILE_PATH, JSON_INVALID_PRODUCE_1_DATA_FILE_PATH, false),
        INVALIDPRODUCE2(JSON_PRODUCE_SCHEMA_FILE_PATH, JSON_INVALID_PRODUCE_2_DATA_FILE_PATH, false),
        INVALIDSTRING(JSON_STRING_SCHEMA_FILE_PATH, JSON_NULL_DATA_FILE_PATH, false),
        INVALIDINTEGER(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_STRING_DATA_FILE_PATH, false),
        EMPLOYEE(JSON_EMPLOYEE_SCHEMA_FILE_PATH, JSON_EMPLOYEE_DATA_FILE_PATH, true),
        WITHOUTSCHEMASPEC(JSON_WITHOUT_SPEC_SCHEMA_FILE_PATH, JSON_INTEGER_DATA_FILE_PATH, true),
        NULLSCHEMASPEC(JSON_NULL_SPEC_SCHEMA_FILE_PATH, JSON_INTEGER_DATA_FILE_PATH, true),
        DATES(JSON_DATES_SCHEMA_FILE_PATH, JSON_DATES_FILE_PATH, true),
        BOOLEAN(JSON_BOOLEAN_SCHEMA_FILE_PATH, JSON_BOOLEAN_FILE_PATH, true),
        DDB(JSON_DDB_SCHEMA_FILE_PATH, JSON_DDB_FILE_PATH, true);

        private final String schemaPath;
        private final String dataPath;
        private final boolean valid;

        TestJsonRecord(String schemaPath,
                       String dataPath,
                       boolean valid) {
            this.schemaPath = schemaPath;
            this.dataPath = dataPath;
            this.valid = valid;
        }

        public String getSchemaPath() {
            return schemaPath;
        }

        public String getDataPath() {
            return dataPath;
        }

        public boolean isValid() {
            return valid;
        }
    }
}
