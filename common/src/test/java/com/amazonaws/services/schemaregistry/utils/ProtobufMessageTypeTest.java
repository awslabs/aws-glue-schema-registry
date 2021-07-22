package com.amazonaws.services.schemaregistry.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ProtobufMessageTypeTest {
    @Test
    public void test_Existing_Enum_Value() {
        assertEquals(ProtobufMessageType.POJO, ProtobufMessageType.fromName("POJO"));
        assertEquals(ProtobufMessageType.DYNAMIC_MESSAGE, ProtobufMessageType.fromName("DYNAMIC_MESSAGE"));
    }

    @Test
    public void test_Non_Existent_Enum_Value() {
        assertThrows(IllegalArgumentException.class, () -> ProtobufMessageType.fromName("Random"));
    }

    @Test
    public void test_Null_Enum_Value() {
        assertThrows(IllegalArgumentException.class, () -> ProtobufMessageType.fromName(""));
    }

    @Test
    public void test_GetName() {
        assertEquals("POJO", ProtobufMessageType.POJO.getName());
    }

    @Test
    public void test_GetValue() {
        assertEquals(1, ProtobufMessageType.POJO.getValue());
    }
}
