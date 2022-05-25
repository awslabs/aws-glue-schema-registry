package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import additionalTypes.Decimals;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.TimeZone;

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

    public static java.util.Date convertFromGoogleDate(com.google.type.Date date) {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        cal.setLenient(false);
        cal.set(Calendar.YEAR, date.getYear());
        cal.set(Calendar.MONTH, date.getMonth() - 1); // Months start at 0, not 1
        cal.set(Calendar.DAY_OF_MONTH, date.getDay());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public static java.util.Date convertFromGoogleTime(com.google.type.TimeOfDay time) {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        //year, month and day must be hardcoded to match the Unix epoch - https://en.wikipedia.org/wiki/Unix_time
        cal.set(Calendar.YEAR, 1969);
        cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DAY_OF_MONTH, 32);
        cal.set(Calendar.HOUR_OF_DAY, time.getHours());
        cal.set(Calendar.MINUTE, time.getMinutes());
        cal.set(Calendar.SECOND, time.getSeconds());
        cal.set(Calendar.MILLISECOND, time.getNanos() / 1000000);
        return cal.getTime();
    }
}
