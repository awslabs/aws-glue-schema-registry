package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata;

import additionalTypes.Decimals;
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils;
import com.google.type.TimeOfDay;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Decimal;
import com.google.protobuf.util.Timestamps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Enum;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;
import lombok.NonNull;
import org.apache.kafka.connect.errors.DataException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE;

/**
 * Converts Protobuf data to Connect data corresponding to the translated schema.
 */
public class ProtobufDataToConnectDataConverter {
    public Object toConnectData(@NonNull final Message message, @NonNull final Schema connectSchema) {

        final List<Field> fields = connectSchema.fields();
        final Struct data = new Struct(connectSchema);
        fields.forEach(
            (field) -> toConnectDataField(field, message, data));

        return data;
    }

    private void toConnectDataField(
        final Field connectField, final Message message, final Struct data) {
        final Schema connectSchema = connectField.schema();
        final Descriptors.FieldDescriptor fieldDescriptor = getFieldByName(message, connectField.name());

        if (fieldDescriptor == null) {
            throw new DataException("Protobuf schema doesn't contain the connect field: " + connectField.name());
        }

        final Object value = message.getField(fieldDescriptor);
        //Unfortunately Protobuf 3 has a complex way to check for optionals.
        final boolean isOptionalFieldNotSet =
            fieldDescriptor.hasOptionalKeyword() && !message.hasField(fieldDescriptor);

        if (value == null || isOptionalFieldNotSet) {
            data.put(connectField, null);
            return;
        }

        try {
            data.put(connectField, toConnectDataField(connectSchema, value));
        } catch (Exception e) {
            throw new DataException(
                    String.format("Error converting value: \"%s\""
                                    + " (Java Type: %s, Protobuf type: %s) to Connect type: %s", value,
                            value.getClass(), fieldDescriptor.getType(), connectSchema.type()), e);
        }
    }

    private Object toConnectDataField(Schema schema, Object value) {
        if (Date.SCHEMA.name().equals(schema.name())) {
            com.google.type.Date date = (com.google.type.Date) value;
            return ProtobufSchemaConverterUtils.convertFromGoogleDate(date);
        }
        if (Timestamp.SCHEMA.name().equals(schema.name())) {
            com.google.protobuf.Timestamp timestamp = (com.google.protobuf.Timestamp) value;
            return Timestamp.toLogical(schema, Timestamps.toMillis(timestamp));
        }
        if (Time.SCHEMA.name().equals(schema.name())) {
            TimeOfDay time = (TimeOfDay) value;
            return ProtobufSchemaConverterUtils.convertFromGoogleTime(time);
        }
        if (Decimal.schema(DECIMAL_DEFAULT_SCALE).name().equals(schema.name())) {
            Decimals.Decimal decimal = (Decimals.Decimal) value;
            return ProtobufSchemaConverterUtils.fromDecimalProto(decimal);
        }
        switch (schema.type()) {
            //TODO: Add this when metadata is added to Protobuf schemas.
            //case INT8:
            //case INT16:
            case INT32: {
                Integer intVal = ((Number) value).intValue();
                return intVal;
            }
            case INT64: {
                long longVal;
                Number number = (Number) value;
                if (value instanceof Long) {
                    longVal = number.longValue();
                } else {
                    longVal = Integer.toUnsignedLong(number.intValue());
                }
                return longVal;
            }
            case FLOAT32: {
                Float floatValue = ((Number) value).floatValue();
                return floatValue;
            }
            case FLOAT64: {
                Double doubleValue = ((Number) value).doubleValue();
                return doubleValue;
            }
            case BOOLEAN: {
                Boolean boolValue = (Boolean) value;
                return boolValue;
            }
            case STRING: {
                if (value instanceof Enum || value instanceof Descriptors.EnumValueDescriptor) {
                    String enumValue = value.toString();
                    return enumValue;
                } else {
                    String strValue = (String) value;
                    return strValue;
                }
            }
            case BYTES: {
                final byte[] valueBytes = ((ByteString) value).toByteArray();
                return valueBytes;
            }
            case ARRAY: {
                final Schema valueSchema = schema.valueSchema();
                final Collection<Object> original = (Collection<Object>) value;
                final List<Object> array = original.stream()
                        .map(elem -> toConnectDataField(valueSchema, elem))
                        .collect(Collectors.toList());
                return array;
            }
            case MAP: {
                final Collection<MapEntry> original = (Collection<MapEntry>) value;
                final Map<Object, Object> map = original.stream().collect(
                        Collectors.toMap(
                                entry -> toConnectDataField(schema.keySchema(), entry.getKey()),
                                entry -> toConnectDataField(schema.valueSchema(), entry.getValue())
                ));
                return map;
            }
            default:
                throw new DataException("Cannot convert unrecognized schema type: " + schema.type());
        }
    }

    private Descriptors.FieldDescriptor getFieldByName(Message message, String fieldName) {
        return message.getDescriptorForType().findFieldByName(fieldName);
    }
}
