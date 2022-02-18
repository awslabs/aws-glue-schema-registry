package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Enum;
import com.google.protobuf.Message;
import lombok.NonNull;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;


import java.util.List;

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
            switch (connectSchema.type()) {
                //TODO: Add this when metadata is added to Protobuf schemas.
                //case INT8:
                //case INT16:
                case INT32: {
                    Integer intVal = ((Number) value).intValue();
                    data.put(connectField, intVal);
                    break;
                }
                case INT64: {
                    long longVal;
                    Number number = (Number) value;
                    if (value instanceof Long) {
                        longVal = number.longValue();
                    } else {
                        longVal = Integer.toUnsignedLong(number.intValue());
                    }
                    data.put(connectField, longVal);
                    break;
                }
                case FLOAT32: {
                    Float floatValue = ((Number) value).floatValue();
                    data.put(connectField, floatValue);
                    break;
                }
                case FLOAT64: {
                    Double doubleValue = ((Number) value).doubleValue();
                    data.put(connectField, doubleValue);
                    break;
                }
                case BOOLEAN: {
                    Boolean boolValue = (Boolean) value;
                    data.put(connectField, boolValue);
                    break;
                }
                case STRING: {
                    if (value instanceof String) {
                        String strValue = (String) value;
                        data.put(connectField, strValue);
                    } else if (value instanceof Enum || value instanceof Descriptors.EnumValueDescriptor) {
                        String enumValue = value.toString();
                        data.put(connectField, enumValue);
                    } else {
                        throw new DataException("Invalid class for string type, expecting String or "
                                + "Enum but found " + value.getClass());
                    }

                    break;
                }
                case BYTES: {
                    final byte[] valueBytes = ((ByteString) value).toByteArray();
                    data.put(connectField, valueBytes);
                    break;
                }
                default:
                    throw new DataException("Cannot convert unrecognized schema type: " + connectSchema.type());
            }
        } catch (Exception e) {
            throw new DataException(
                String.format("Error converting value: \"%s\""
                        + " (Java Type: %s, Protobuf type: %s) to Connect type: %s", value,
                    value.getClass(), fieldDescriptor.getType(), connectSchema.type()), e);
        }
    }

    private Descriptors.FieldDescriptor getFieldByName(Message message, String fieldName) {
        return message.getDescriptorForType().findFieldByName(fieldName);
    }
}
