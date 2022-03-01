package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.Descriptors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_TYPE;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.BOOL;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.BYTES;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.DOUBLE;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FLOAT;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.INT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.INT64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectDataToProtobufDataConverterFactory {

    private static final List<Descriptors.FieldDescriptor.Type> PROTOBUF_PRIMITIVE_TYPES =
            Arrays.asList(BOOL, BYTES, DOUBLE, FIXED32, FIXED64, FLOAT, INT32, INT64, SFIXED32, SFIXED64, SINT32, SINT64, STRING, UINT32, UINT64);

    //public static DataConverter get(final Schema connectSchema) {
    public static DataConverter get(final Descriptors.FieldDescriptor fieldDescriptor) {
        final Descriptors.FieldDescriptor.Type protobufType = fieldDescriptor.getType();

        if (protobufType.equals(PROTOBUF_ENUM_TYPE)) {
            return new EnumDataConverter();
        } else if (PROTOBUF_PRIMITIVE_TYPES.contains(protobufType)) { //primitive case
            return new PrimitiveDataConverter();
        }

        throw new IllegalArgumentException("Unrecognized connect type: " + protobufType);
    }
}

