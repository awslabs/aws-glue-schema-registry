package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;

public class ProtobufSchemaConverterUtils {

    private static final String MAP_ENTRY_SUFFIX = "Entry";

    public static String toMapEntryName(String s) {
        if (s.contains("_")) {
            s = LOWER_UNDERSCORE.to(UPPER_CAMEL, s);
        }
        s += MAP_ENTRY_SUFFIX;
        s = s.substring(0, 1).toUpperCase() + s.substring(1);
        return s;
    }
}
