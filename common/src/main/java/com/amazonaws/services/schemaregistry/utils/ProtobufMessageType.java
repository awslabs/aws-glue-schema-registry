package com.amazonaws.services.schemaregistry.utils;

import org.apache.commons.lang3.StringUtils;

/**
 *  Defines a set of supported Data Formats for Protobuf Messages
 */
public enum ProtobufMessageType {
    /**
     * Unknown
     */
    UNKNOWN("UNKNOWN", 0),

    /**
     * Pojo object type
     */
    POJO("POJO", 1),

    /**
     * DynamicMessage object type
     */
    DYNAMIC_MESSAGE("DYNAMIC_MESSAGE", 2);

    private final String name;
    private final int value;

    public String getName() {
        return name;
    }

    public int getValue() {
        return value;
    }

    ProtobufMessageType(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public static ProtobufMessageType fromName(String name) {
        if (!StringUtils.isEmpty(name)) {
            name = name.toUpperCase();
        }
        return valueOf(name);
    }
}
