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
package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroDataBigDecimalTest {
    private AvroData avroData;

    @BeforeEach
    void setUp() {
        avroData = new AvroData(2);
    }

    @Test
    void testToConnectDataWithBigDecimalValue() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL);
        avroSchema.addProp("precision", 50);
        avroSchema.addProp("scale", 2);

        BigDecimal testDecimal = new BigDecimal(new BigInteger("156"), 2);

        SchemaAndValue expected = new SchemaAndValue(
                Decimal.builder(2).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "50").build(),
                testDecimal
        );

        SchemaAndValue actual = avroData.toConnectData(avroSchema, testDecimal);

        assertEquals(expected.schema(), actual.schema());
        assertEquals(expected.value(), actual.value());
    }
}
