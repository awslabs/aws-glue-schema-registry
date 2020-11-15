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

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class AVROUtilsTest {
    private AWSSchemaRegistryClient mockClient;
    private final Map<String, Object> configs = new HashMap<>();
    private Schema schema = null;
    private User userDefinedPojo;
    private GenericRecord genericRecord;
    private Customer customer;

    @BeforeEach
    public void setup() {
        mockClient = mock(AWSSchemaRegistryClient.class);

        customer = new Customer();
        customer.setName("test");

        Schema.Parser parser = new Schema.Parser();
        try {
            schema = parser.parse(new File("src/test/java/resources/avro/user.avsc"));

        } catch (IOException e) {
            fail("Catch IOException: ", e);
        }

        userDefinedPojo = User.newBuilder().setName("test_avros_schema").setFavoriteColor("violet")
                .setFavoriteNumber(10).build();

        genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", "sansa");
        genericRecord.put("favorite_number", 99);
        genericRecord.put("favorite_color", "red");

        configs.put("endpoint", "https://mjguu1u07a.execute-api.us-west-2.amazonaws.com/beta");
        configs.put("region", "us-west-2");
        configs.put("compression", "NONE");
    }

    @Test
    public void testGetSchemaDefinition_pojo_schemaDefinitionMatches() {
        String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.amazonaws.services.schemaregistry.serializers.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";
        assertEquals(schema, AVROUtils.getInstance().getSchemaDefinition(userDefinedPojo));
    }

    @Test
    public void testGetSchemaDefinition_parseSchema_schemaDefinitionMatches() {
        String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.amazonaws.services.schemaregistry.serializers.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";
        assertEquals(schema, AVROUtils.getInstance().getSchemaDefinition(genericRecord));
    }

    @Test
    public void testGetSchemaDefinition_nonAVROSchema_throwsException() {
        Assertions.assertThrows(AWSSchemaRegistryException.class,
                () -> assertNull(AVROUtils.getInstance().getSchemaDefinition(customer)));
    }

    @Test
    public void testGetSchemaDefinition_nullObject_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AVROUtils.getInstance().getSchemaDefinition(null));
    }

    @Test
    public void testGetSchema_nullObject_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AVROUtils.getInstance().getSchema(null));
    }
}

class Customer {
    String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
