package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;


public class TimeDataConverter implements DataConverter {

    @Override
    public void toProtobufData(Schema schema,
                               Object value,
                               Descriptors.FieldDescriptor fieldDescriptor,
                               Message.Builder messageBuilder) {
        messageBuilder.setField(fieldDescriptor, toProtobufData(schema, value, fieldDescriptor));
    }

    @Override
    public Object toProtobufData(Schema schema, Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (Date.SCHEMA.name().equals(schema.name())) {
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            Calendar cal = Calendar.getInstance(timeZone);
            cal.setTime((java.util.Date) value);
            com.google.type.Date.Builder dateBuilder = com.google.type.Date.newBuilder();
            dateBuilder.setDay(cal.get(Calendar.DAY_OF_MONTH));
            dateBuilder.setMonth(cal.get(Calendar.MONTH) + 1); //Months start at 0
            dateBuilder.setYear(cal.get(Calendar.YEAR));
            return dateBuilder.build();
        } else if (Time.SCHEMA.name().equals(schema.name())) {
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            Calendar cal = Calendar.getInstance(timeZone);
            cal.setTime((java.util.Date) value);
            com.google.type.TimeOfDay.Builder timeBuilder = com.google.type.TimeOfDay.newBuilder();
            timeBuilder.setHours(cal.get(Calendar.HOUR_OF_DAY));
            timeBuilder.setMinutes(cal.get(Calendar.MINUTE));
            timeBuilder.setSeconds(cal.get(Calendar.SECOND));
            timeBuilder.setNanos(cal.get(Calendar.MILLISECOND) * 1000000); //Converting milliseconds to nanoseconds
            return timeBuilder.build();
        } else if (Timestamp.SCHEMA.name().equals(schema.name())) {
            return Timestamps.fromMillis(Timestamp.fromLogical(schema, (java.util.Date) value));
        }

        throw new DataException(String.format("Invalid schema type %s for value %s", schema.type(), value.getClass()));
    }
}
