package com.amazonaws.services.schemaregistry.utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroRecordTypeTest {

    @Test
    public void testForName_succeeds() {
        assertEquals(AvroRecordType.UNKNOWN, AvroRecordType.fromName("UNKNOWN"));
        assertEquals(AvroRecordType.SPECIFIC_RECORD, AvroRecordType.fromName("SPECIFIC_RECORD"));
        assertEquals(AvroRecordType.GENERIC_RECORD, AvroRecordType.fromName("GENERIC_RECORD"));
    }

    @Test
    public void testGetValue_returnsCorrectValue() {
        assertEquals(0, AvroRecordType.UNKNOWN.getValue());
        assertEquals(1, AvroRecordType.SPECIFIC_RECORD.getValue());
        assertEquals(2, AvroRecordType.GENERIC_RECORD.getValue());
    }
}