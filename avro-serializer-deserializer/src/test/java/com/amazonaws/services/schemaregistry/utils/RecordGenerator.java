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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public final class RecordGenerator {
    public static final String AVRO_USER_SCHEMA_FILE_PATH = "src/test/java/resources/avro/user.avsc";
    public static final String AVRO_EMP_RECORD_SCHEMA_FILE_PATH = "src/test/java/resources/avro/emp_record.avsc";

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return Generic AVRO Record
     */
    public static GenericRecord createGenericAvroRecord() {
        Schema schema = SchemaLoader.loadSchema(AVRO_USER_SCHEMA_FILE_PATH);
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
        return User
                .newBuilder()
                .setName("test")
                .setFavoriteColor("violet")
                .setFavoriteNumber(10)
                .build();
    }

    /**
     * Helper method to generate a test GenericRecord from emp record schema
     * @return Generic AVRO Record
     */
    public static GenericRecord createGenericEmpRecord() {
        Schema schema = SchemaLoader.loadSchema(AVRO_EMP_RECORD_SCHEMA_FILE_PATH);
        GenericRecord genericRecord  =  new GenericData.Record(schema);
        genericRecord.put("name", "xyz");
        genericRecord.put("id", 001);
        genericRecord.put("salary",30000);
        genericRecord.put("age", 25);
        genericRecord.put("address", "abc");
        return genericRecord;
    }
}
