package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Set;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_NAME;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_VALUE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_PACKAGE;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.ENUM;
/**
 * Converts the Protobuf schema to Connect schemas.
 * Partially inspired from https://github.com/blueapron/kafka-connect-protobuf-converter/blob/master/src/main/java/com/blueapron/connect/protobuf/ProtobufData.java#L135
 */
public class ProtobufSchemaToConnectSchemaConverter {

    private static final Set<Descriptors.FieldDescriptor.Type> TYPES_TO_ADD_METADATA =
        ImmutableSet.<Descriptors.FieldDescriptor.Type>builder()
            .add(SINT32, SFIXED32, UINT32, UINT64, FIXED32, FIXED64, SFIXED64, SINT64).build();
    private static final Integer CONVERTER_VERSION = 1;

    public Schema toConnectSchema(@NonNull final Message message) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        final Descriptors.Descriptor descriptor = message.getDescriptorForType();
        final List<Descriptors.FieldDescriptor> fieldDescriptorList = descriptor.getFields();

        builder.name(descriptor.getName());
        builder.version(CONVERTER_VERSION);
        builder.parameter(PROTOBUF_PACKAGE, descriptor.getFile().getPackage());

        for (final Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptorList) {
            final String fieldName = fieldDescriptor.getName();
            builder.field(fieldName, toConnectSchemaForField(fieldDescriptor));
        }

        //TODO: Add support for reading metadata from Protobuf schemas for disambiguating between INT8 and INT16

        return builder.build();
    }

    private Schema toConnectSchemaForField(final Descriptors.FieldDescriptor fieldDescriptor) {
        return toConnectSchemaBuilderForField(fieldDescriptor).build();
    }

    private SchemaBuilder toConnectSchemaBuilderForField(final Descriptors.FieldDescriptor fieldDescriptor) {
        final Descriptors.FieldDescriptor.Type protobufType = fieldDescriptor.getType();

        SchemaBuilder schemaBuilder = null;

        switch (protobufType) {
            case INT32:
            case SINT32:
            case SFIXED32: {
                schemaBuilder = SchemaBuilder.int32();
                break;
            }
            case INT64:
            case SINT64:
            case UINT64:
            case FIXED64:
            case SFIXED64:
            case UINT32:
            case FIXED32: {
                schemaBuilder = SchemaBuilder.int64();
                break;
            }
            case FLOAT: {
                schemaBuilder = SchemaBuilder.float32();
                break;
            }
            case DOUBLE: {
                schemaBuilder = SchemaBuilder.float64();
                break;
            }
            case BOOL: {
                schemaBuilder = SchemaBuilder.bool();
                break;
            }
            case ENUM: //ENUM will be converted into a string in Connect, as Connect does not support ENUM. data stored in metadata (see below)
            case STRING: {
                schemaBuilder = SchemaBuilder.string();
                break;
            }
            case BYTES: {
                schemaBuilder = SchemaBuilder.bytes();
                break;
            }
            case MESSAGE: {
                if (fieldDescriptor.isMapField()) {
                    Descriptors.Descriptor mapDescriptor = fieldDescriptor.getMessageType();
                    Descriptors.FieldDescriptor keyFieldDescriptor = mapDescriptor.findFieldByName("key");
                    Descriptors.FieldDescriptor valueFieldDescriptor = mapDescriptor.findFieldByName("value");
                    schemaBuilder = SchemaBuilder.map(
                            toConnectSchemaBuilderForField(keyFieldDescriptor).optional().build(),
                            toConnectSchemaBuilderForField(valueFieldDescriptor).optional().build());
                }
                break;
            }
            default:
                throw new DataException("Invalid Protobuf type passed: " + protobufType);
        }

        //Protobuf provides different types of integers.
        //We add metadata to Connect schema to store the original type used.
        if (TYPES_TO_ADD_METADATA.contains(protobufType)) {
            schemaBuilder.parameter(PROTOBUF_TYPE, protobufType.name().toUpperCase());
        } else if (protobufType.equals(ENUM)) { //ENUM case; storing ENUM data as metadata to avoid being lost in translation.
            schemaBuilder.parameter(PROTOBUF_TYPE, PROTOBUF_ENUM_TYPE);
            for (Descriptors.EnumValueDescriptor enumValueDescriptor: fieldDescriptor.getEnumType().getValues()) { //iterating through the values of the Enum to store each one
                schemaBuilder.parameter(PROTOBUF_ENUM_VALUE + enumValueDescriptor.getName(), String.valueOf(enumValueDescriptor.getNumber()));
            }
            schemaBuilder.parameter(PROTOBUF_ENUM_NAME, fieldDescriptor.getName());
        }

        if (fieldDescriptor.hasOptionalKeyword()) {
            schemaBuilder.optional();
        }

        if (fieldDescriptor.isRepeated() && schemaBuilder.type() != Schema.Type.MAP) {
            Schema schema = schemaBuilder.build();
            schemaBuilder = SchemaBuilder.array(schema).optional();
            schemaBuilder.parameter(PROTOBUF_TAG, String.valueOf(fieldDescriptor.getNumber()));
            return schemaBuilder;
        }

        schemaBuilder.parameter(PROTOBUF_TAG, String.valueOf(fieldDescriptor.getNumber()));

        return schemaBuilder;
    }
}
