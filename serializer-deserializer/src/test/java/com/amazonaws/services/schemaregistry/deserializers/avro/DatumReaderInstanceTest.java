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
package com.amazonaws.services.schemaregistry.deserializers.avro;

import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatumReaderInstanceTest {

    /** Resolves to vofcanary.Plain, which is on the test classpath and is not a SpecificRecord. */
    private static final String PROBE_SCHEMA =
        "{\"type\":\"record\",\"name\":\"Plain\",\"namespace\":\"vofcanary\",\"fields\":[]}";

    private static final String USER_SCHEMA =
        "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.example\","
            + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";

    /**
     * A writer schema's namespace and name select the reader class, so a schema author picks which
     * class is resolved. Resolving one that is not a SpecificRecord must fail, and must fail
     * without initializing or constructing it: {@code Class.newInstance()} does both, so checking
     * only after the call leaves the attacker's code already run.
     */
    @Test
    public void testFrom_WhenSchemaNamesNonSpecificRecordClass_ThrowsAndNeitherInitializesNorConstructsIt() {
        // Catch Throwable rather than asserting the type up front, so that the two security
        // assertions below are reached whatever this throws. Asserting the type first would, on a
        // regression, report "unexpected exception type" and never reveal that the probe ran.
        Throwable thrown = null;
        try {
            DatumReaderInstance.from(PROBE_SCHEMA, AvroRecordType.SPECIFIC_RECORD);
        } catch (Throwable t) {
            thrown = t;
        }

        assertFalse(SpecificRecordProbeFlag.staticInitRan(),
            "vofcanary.Plain was initialized, so a schema can still run a static initializer "
                + "on the consumer");
        assertFalse(SpecificRecordProbeFlag.constructed(),
            "vofcanary.Plain was constructed, so a schema can still run a constructor "
                + "on the consumer");

        assertTrue(thrown instanceof IllegalArgumentException,
            "expected the resolution to be refused, but got: " + thrown);
        assertEquals("Avro schema resolves to class vofcanary.Plain, which is not a "
            + "org.apache.avro.specific.SpecificRecord.", thrown.getMessage());
    }

    /**
     * GENERIC_RECORD never resolves a class from the schema, so it must stay unaffected.
     */
    @Test
    public void testFrom_GenericRecord_ResolvesNoClassFromSchema() throws Exception {
        DatumReader<Object> reader = DatumReaderInstance.from(PROBE_SCHEMA, AvroRecordType.GENERIC_RECORD);

        assertTrue(reader instanceof GenericDatumReader);
        assertFalse(SpecificRecordProbeFlag.staticInitRan());
        assertFalse(SpecificRecordProbeFlag.constructed());
    }

    /**
     * A schema naming a class that is absent from the classpath must report that plainly rather
     * than surfacing a ClassCastException or NullPointerException from further down.
     */
    @Test
    public void testFrom_WhenSpecificRecordClassIsNotOnClasspath_ThrowsIllegalArgument() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> DatumReaderInstance.from(USER_SCHEMA, AvroRecordType.SPECIFIC_RECORD));

        assertEquals("Avro schema resolves to class com.example.User, which is not on the classpath.",
            ex.getMessage());
    }

    /**
     * A generated SpecificRecord still deserializes, which is the compatibility case that matters:
     * the check must not block legitimate SPECIFIC_RECORD consumers.
     */
    @Test
    public void testFrom_WhenSchemaNamesGeneratedSpecificRecord_ReturnsSpecificDatumReader()
        throws Exception {
        String schema = com.amazonaws.services.schemaregistry.serializers.avro.User.SCHEMA$.toString();

        DatumReader<Object> reader = DatumReaderInstance.from(schema, AvroRecordType.SPECIFIC_RECORD);

        assertTrue(reader instanceof SpecificDatumReader);
    }

    @Test
    public void testFrom_UnsupportedRecordType_Throws() {
        assertThrows(UnsupportedOperationException.class,
            () -> DatumReaderInstance.from(USER_SCHEMA, AvroRecordType.UNKNOWN));
    }
}
