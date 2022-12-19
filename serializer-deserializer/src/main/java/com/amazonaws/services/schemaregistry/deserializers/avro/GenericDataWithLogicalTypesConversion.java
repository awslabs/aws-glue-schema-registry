package com.amazonaws.services.schemaregistry.deserializers.avro;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

@Slf4j
public class GenericDataWithLogicalTypesConversion {
    private static final GenericData INSTANCE = new GenericData();

    static {
        INSTANCE.addLogicalTypeConversion(new Conversions.DecimalConversion());
        INSTANCE.addLogicalTypeConversion(new Conversions.UUIDConversion());
        INSTANCE.addLogicalTypeConversion(new TimeConversions.DateConversion());
        INSTANCE.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        INSTANCE.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        INSTANCE.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        INSTANCE.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        INSTANCE.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        INSTANCE.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    }

    public static GenericData getInstance() {
        return INSTANCE;
    }
}
