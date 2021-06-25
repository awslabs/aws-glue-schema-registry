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
package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Factory Test to create a new instance of TypeConverter.
 */
public class TypeConverterFactoryTest {
    private final TypeConverterFactory typeConverterFactory = new TypeConverterFactory();

    private static Stream<Arguments> testLogicalNameArgumentsProvider() {
        return Stream.of(Arguments.of(Decimal.LOGICAL_NAME), Arguments.of(Date.LOGICAL_NAME),
                         Arguments.of(Time.LOGICAL_NAME), Arguments.of(Timestamp.LOGICAL_NAME));
    }

    /**
     * Test for Type Converter instance creation.
     */
    @ParameterizedTest
    @EnumSource(value = Schema.Type.class)
    public void testGetTypeConverter_createObject_succeeds(Schema.Type schemaType) {
        TypeConverter typeConverter = typeConverterFactory.get(schemaType);

        assertNotNull(typeConverter);
        assertTrue(typeConverter.getClass()
                           .getSimpleName()
                           .toLowerCase()
                           .startsWith(schemaType.name()
                                               .toLowerCase()));
    }

    /**
     * Test for type converter instance by logical name
     */
    @ParameterizedTest
    @MethodSource("testLogicalNameArgumentsProvider")
    public void testGetTypeConverter_ByLogicalName_succeeds(String logicalName) {
        TypeConverter typeConverter = typeConverterFactory.get(logicalName);
        assertNotNull(typeConverterFactory.get(logicalName));

        String[] logicalNameSplit = logicalName.split("\\.");

        assertTrue(typeConverter.getClass()
                           .getSimpleName()
                           .startsWith(logicalNameSplit[logicalNameSplit.length - 1]));
    }

    /**
     * Test for unknown type converter instance
     */
    @Test
    public void testGetTypeConverter_UnsupportedType_returnsNull() {
        assertNull(typeConverterFactory.get("INT128"));
    }
}
