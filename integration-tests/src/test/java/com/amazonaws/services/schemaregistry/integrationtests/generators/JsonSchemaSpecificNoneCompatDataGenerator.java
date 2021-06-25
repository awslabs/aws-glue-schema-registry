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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Utility functions class for the generating Specific JSON Schema none compatible records
 */
public class JsonSchemaSpecificNoneCompatDataGenerator implements TestDataGenerator<Car> {
    /**
     * Method to generate Specific JSON Records
     *
     * @return List<Car>
     */
    @Override
    public List<Car> createRecords() {
        Car honda = Car.builder()
                .make("Honda")
                .model("crv")
                .used(true)
                .miles(10000)
                .year(2016)
                .listedDate(new GregorianCalendar(2014, Calendar.FEBRUARY, 11).getTime())
                .purchaseDate(Date.from(Instant.parse("2000-01-01T00:00:00.000Z")))
                .owners(new String[]{"John", "Jane", "Hu"})
                .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
                .build();

        Car bmw = Car.builder()
                .make("Tesla")
                .model("3")
                .used(false)
                .miles(20000)
                .year(2020)
                .listedDate(new GregorianCalendar(2020, Calendar.FEBRUARY, 20).getTime())
                .purchaseDate(Date.from(Instant.parse("2000-01-01T00:00:00.000Z")))
                .owners(new String[]{"Harry", "Megan"})
                .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
                .build();

        List<Car> records = new ArrayList<>();
        records.add(honda);
        records.add(bmw);

        return records;
    }
}
