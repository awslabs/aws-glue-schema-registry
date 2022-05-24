package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;
import additionalTypes.Decimals;

import java.math.BigDecimal;

public class DecimalDataConverter implements DataConverter {
    @Override
    public void toProtobufData(final Schema schema,
                               final Object value,
                               final Descriptors.FieldDescriptor fieldDescriptor,
                               final Message.Builder messageBuilder) {
        messageBuilder.setField(fieldDescriptor, toProtobufData(schema, value, fieldDescriptor));
    }

    @Override
    public Object toProtobufData(Schema schema, Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        final Decimals.Decimal.Builder decimalBuilder = Decimals.Decimal.newBuilder();
        BigDecimal decimalValue = (BigDecimal) value;
//        decimalBuilder.setUnits(decimalValue.intValue());
//        decimalBuilder.setFraction(decimalValue.remainder(BigDecimal.ONE).multiply(BigDecimal.valueOf(1000000000)).intValue());
//        decimalBuilder.setPrecision(decimalValue.precision());
//        decimalBuilder.setScale(decimalValue.scale());
        return ProtobufSchemaConverterUtils.fromBigDecimal(decimalValue);
        //return decimalBuilder.build();
    }
}