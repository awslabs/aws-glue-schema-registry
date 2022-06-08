/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata;

import additionalTypes.Decimals;
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.type.TimeOfDay;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Decimal;
import com.google.protobuf.util.Timestamps;
import lombok.NonNull;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ONEOF_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;

/**
 * Converts Protobuf data to Connect data corresponding to the translated schema.
 */
public class ProtobufDataToConnectDataConverter {
    public Object toConnectData(@NonNull final Message message, @NonNull final Schema connectSchema) {

        final List<Field> fields = connectSchema.fields();
        final Struct data = new Struct(connectSchema);

        for (Field field : fields) {
            if (field.schema().type().equals(Schema.Type.STRUCT)
                    && field.schema().parameters().containsKey(PROTOBUF_TYPE)
                    && field.schema().parameters().get(PROTOBUF_TYPE).equals(PROTOBUF_ONEOF_TYPE)) {
                Struct oneof = new Struct(field.schema());
                for (Field oneofField : field.schema().fields()) {
                    toConnectDataField(oneofField, message, oneof);
                }
                data.put(field, oneof);
            } else {
                toConnectDataField(field, message, data);
            }
        }

        return data;
    }

    private void toConnectDataField(
        final Field connectField, final Message message, final Struct data) {
        final Schema connectSchema = connectField.schema();
        final Descriptors.FieldDescriptor fieldDescriptor = getFieldByName(message, connectField.name());

        if (fieldDescriptor == null) {
            throw new DataException("Protobuf schema doesn't contain the connect field: " + connectField.name());
        }

        if (fieldDescriptor.getRealContainingOneof() != null && !message.hasField(fieldDescriptor)) {
            // Skip the NONE or NOT_SET oneof field
            return;
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

    @SneakyThrows
    private Object toConnectDataField(Schema schema, Object value) {
        if (Date.SCHEMA.name().equals(schema.name())) {
            com.google.type.Date date = com.google.type.Date.parseFrom(((Message) value).toByteArray());
            return ProtobufSchemaConverterUtils.convertFromGoogleDate(date);
        }
        if (Timestamp.SCHEMA.name().equals(schema.name())) {
            com.google.protobuf.Timestamp timestamp =
                    com.google.protobuf.Timestamp.parseFrom(((Message) value).toByteArray());
            return Timestamp.toLogical(schema, Timestamps.toMillis(timestamp));
        }
        if (Time.SCHEMA.name().equals(schema.name())) {
            TimeOfDay time = TimeOfDay.parseFrom(((Message) value).toByteArray());
            return ProtobufSchemaConverterUtils.convertFromGoogleTime(time);
        }
        if (Decimal.schema(DECIMAL_DEFAULT_SCALE).name().equals(schema.name())) {
            Decimals.Decimal decimal = Decimals.Decimal.parseFrom(((Message) value).toByteArray());
            return fromDecimalProto(decimal);
        }
        switch (schema.type()) {
            case INT8:
                Byte byteVal = ((Number) value).byteValue();
                return byteVal;
            case INT16:
                Short shortVal = ((Number) value).shortValue();
                return shortVal;
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
                final Collection<Message> original = (Collection<Message>) value;
                final Map<Object, Object> map = original.stream().collect(
                        Collectors.toMap(
                                entry -> toConnectDataField(schema.keySchema(), getMapField(entry, "key")),
                                entry -> toConnectDataField(schema.valueSchema(), getMapField(entry, "value"))
                ));
                return map;
            }
            case STRUCT: {
                return toConnectData((Message) value, schema.schema());
            }
            default:
                throw new DataException("Cannot convert unrecognized schema type: " + schema.type());
        }
    }

    private Object getMapField(Message mapEntry, String fieldName) {
        Descriptors.FieldDescriptor field = getFieldByName(mapEntry, fieldName);
        return mapEntry.getField(field);
    }

    private Descriptors.FieldDescriptor getFieldByName(Message message, String fieldName) {
        return message.getDescriptorForType().findFieldByName(fieldName);
    }
}
