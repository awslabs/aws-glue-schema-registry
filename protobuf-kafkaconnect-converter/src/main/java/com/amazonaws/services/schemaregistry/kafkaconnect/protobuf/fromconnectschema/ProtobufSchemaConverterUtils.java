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

    public static Decimals.Decimal fromBigDecimal(BigDecimal bigDecimal) {
        return Decimals.Decimal
                .newBuilder()
                .setUnits(bigDecimal.intValue())
                .setFraction(bigDecimal.remainder(BigDecimal.ONE).multiply(BigDecimal.valueOf(1000000000)).intValue())
                .setPrecision(bigDecimal.precision())
                .setScale(bigDecimal.scale())
                .build();
    }

    public static BigDecimal fromDecimalProto(Decimals.Decimal decimal) {

        MathContext precisionMathContext = new MathContext(decimal.getPrecision(), RoundingMode.UNNECESSARY);
        BigDecimal units = new BigDecimal(decimal.getUnits(), precisionMathContext);

        BigDecimal fractionalPart = new BigDecimal(decimal.getFraction(), precisionMathContext);
        BigDecimal fractionalUnits = new BigDecimal(1000000000, precisionMathContext);
        //Set the right scale for fractional part. Make sure we ignore the digits beyond the scale.
        fractionalPart =
                fractionalPart.divide(fractionalUnits, precisionMathContext)
                        .setScale(decimal.getScale(), RoundingMode.UNNECESSARY);

        return units.add(fractionalPart);
    }
}
