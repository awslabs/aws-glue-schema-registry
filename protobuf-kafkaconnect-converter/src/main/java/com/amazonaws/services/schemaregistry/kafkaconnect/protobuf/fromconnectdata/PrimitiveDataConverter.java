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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32;

public class PrimitiveDataConverter implements DataConverter {
    private static final List<Descriptors.FieldDescriptor.Type> INT32_METADATA_TYPES = Arrays.asList(UINT32, FIXED32);

    @Override
    public void toProtobufData(final Descriptors.FileDescriptor fileDescriptor, final Schema schema,
                               final Object value, final Descriptors.FieldDescriptor fieldDescriptor,
                               final Message.Builder messageBuilder) {
        messageBuilder.setField(fieldDescriptor, toProtobufData(fileDescriptor, schema, value, fieldDescriptor));
    }

    @Override
    public Object toProtobufData(final Descriptors.FileDescriptor fileDescriptor, final Schema schema,
                                 final Object value, final Descriptors.FieldDescriptor fieldDescriptor) {
        final Schema.Type schemaType = schema.type();
        try {
            switch (schemaType) {
                case INT8: {
                    final Integer intValue = (Byte.valueOf((byte) value)).intValue();
                    return intValue;
                }
                case INT16: {
                    final Integer intValue = (Short.valueOf((short) value)).intValue();
                    return intValue;
                }
                case INT32: {
                    final Integer intValue = (Integer) value;
                    return intValue;
                }
                case INT64: {
                    if (INT32_METADATA_TYPES.contains(fieldDescriptor.getType())) {
                        //If type metadata is set to one of the 32-bit types.
                        final int intValue = (int) ((Number) value).longValue();
                        return intValue;
                    }
                    final Long longValue = (Long) value;
                    return longValue;
                }
                case FLOAT32: {
                    final Float floatValue = (Float) value;
                    return floatValue;
                }
                case FLOAT64: {
                    final Double doubleValue = (Double) value;
                    return doubleValue;
                }
                case BOOLEAN: {
                    final Boolean boolValue = (Boolean) value;
                    return boolValue;
                }
                case STRING: {
                    final String stringValue = (String) value;
                    return stringValue;
                }
                case BYTES: {
                    final ByteBuffer bytesValue = value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) :
                        (ByteBuffer) value;
                    return ByteString.copyFrom(bytesValue);
                }
                default:
                    throw new DataException(String
                        .format("Unknown schema type: %s for field %s", schema.type(), fieldDescriptor.getName()));
            }
        } catch (ClassCastException e) {
            throw new DataException(
                String.format("Invalid schema type %s for value %s", schema.type(), value.getClass()));
        }
    }
}
