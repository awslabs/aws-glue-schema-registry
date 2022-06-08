package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import additionalTypes.Decimals;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;

public class DecimalDataConverter implements DataConverter {
    @Override
    public void toProtobufData(final Descriptors.FileDescriptor fileDescriptor, final Schema schema,
                               final Object value, final Descriptors.FieldDescriptor fieldDescriptor,
                               final Message.Builder messageBuilder) {
        messageBuilder.setField(fieldDescriptor, toProtobufData(fileDescriptor, schema, value, fieldDescriptor));
    }

    @Override
    public Object toProtobufData(final Descriptors.FileDescriptor fileDescriptor, final Schema schema,
                                 final Object value, final Descriptors.FieldDescriptor fieldDescriptor) {
        BigDecimal decimalValue = (BigDecimal) value;
        return fromBigDecimal(decimalValue);
    }

    public static Decimals.Decimal fromBigDecimal(BigDecimal bigDecimal) {
        int units = bigDecimal.intValue();
        int fraction = bigDecimal.remainder(BigDecimal.ONE).multiply(BigDecimal.valueOf(1000000000)).intValue();
        int precision = bigDecimal.precision();
        int scale = bigDecimal.scale();
        return Decimals.Decimal
                .newBuilder()
                .setUnits(units)
                .setFraction(fraction)
                .setPrecision(precision)
                .setScale(scale)
                .build();
    }
}