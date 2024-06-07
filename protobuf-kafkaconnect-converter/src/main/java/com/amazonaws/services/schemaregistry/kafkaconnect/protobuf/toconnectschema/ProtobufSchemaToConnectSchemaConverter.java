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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.errors.DataException;
import metadata.ProtobufSchemaMetadata;

import java.util.List;
import java.util.Set;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_INT16;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_INT8;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_SCALE_VALUE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_NAME;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_VALUE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ONEOF_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_PACKAGE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE;
import static com.google.protobuf.Descriptors.FieldDescriptor.Type.*;

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

        processFieldDescriptors(builder, fieldDescriptorList);

        return builder.build();
    }

    private void processFieldDescriptors(SchemaBuilder builder, List<Descriptors.FieldDescriptor> fieldDescriptorList) {
        for (final Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptorList) {
            if (fieldDescriptor.getRealContainingOneof() != null) {
                Descriptors.OneofDescriptor oneofDescriptor = fieldDescriptor.getRealContainingOneof();
                if (!builder.fields().stream().anyMatch(field -> field.name().equals(oneofDescriptor.getName()))) {
                    builder.field(oneofDescriptor.getName(), toConnectSchemaForOneOfField(oneofDescriptor));
                }
            } else {
                final String fieldName = fieldDescriptor.getName();
                builder.field(fieldName, toConnectSchemaForField(fieldDescriptor));
            }
        }
    }

    private Schema toConnectSchemaForField(final Descriptors.FieldDescriptor fieldDescriptor) {
        return toConnectSchemaBuilderForField(fieldDescriptor).build();
    }

    private Schema toConnectSchemaForOneOfField(final Descriptors.OneofDescriptor oneofDescriptor) {
        SchemaBuilder builder = SchemaBuilder.struct().name(oneofDescriptor.getName());
        for (Descriptors.FieldDescriptor fieldDescriptor : oneofDescriptor.getFields()) {
            builder.field(fieldDescriptor.getName(),
                    toConnectSchemaBuilderForField(fieldDescriptor).optional().build());
        }
        builder.parameter(PROTOBUF_TYPE, PROTOBUF_ONEOF_TYPE);
        builder.optional();
        return builder.build();
    }

    private SchemaBuilder toConnectSchemaBuilderForField(final Descriptors.FieldDescriptor fieldDescriptor) {
        final Descriptors.FieldDescriptor.Type protobufType = fieldDescriptor.getType();

        SchemaBuilder schemaBuilder = null;

        switch (protobufType) {
            case INT32:
            case SINT32:
            case SFIXED32: {
                if (fieldDescriptor.getOptions().hasExtension(ProtobufSchemaMetadata.metadataKey)
                        && fieldDescriptor.getOptions().hasExtension(ProtobufSchemaMetadata.metadataValue)) {
                    String metadataKey = fieldDescriptor.getOptions().getExtension(ProtobufSchemaMetadata.metadataKey);
                    String metadataValue = fieldDescriptor.getOptions().getExtension(ProtobufSchemaMetadata.metadataValue);
                    if (metadataKey.equals(CONNECT_SCHEMA_TYPE)) {
                        if (CONNECT_SCHEMA_INT8.equals(metadataValue)) {
                            schemaBuilder = SchemaBuilder.int8();
                            break;
                        } else if (CONNECT_SCHEMA_INT16.equals(metadataValue)) {
                            schemaBuilder = SchemaBuilder.int16();
                            break;
                        }
                    }
                }
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
            // ENUM will be converted into a string in Connect, as Connect does not support ENUM. data stored in metadata (see below)
            case ENUM:
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
                    break;
                }
                if (fieldDescriptor.getMessageType().getFullName().equals("google.type.Date")) {
                    schemaBuilder = Date.builder();
                    break;
                }
                if (fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp")) {
                    schemaBuilder = Timestamp.builder();
                    break;
                }
                if (fieldDescriptor.getMessageType().getFullName().equals("google.type.TimeOfDay")) {
                    schemaBuilder = Time.builder();
                    break;
                }
                if (fieldDescriptor.getMessageType().getFullName().equals("additionalTypes.Decimal")) {
                    if (fieldDescriptor.getOptions().hasExtension(ProtobufSchemaMetadata.metadataKey)
                            && fieldDescriptor.getOptions().hasExtension(ProtobufSchemaMetadata.metadataValue)) {
                        String metadataKey = fieldDescriptor.getOptions().getExtension(ProtobufSchemaMetadata.metadataKey);
                        String metadataValue = fieldDescriptor.getOptions().getExtension(ProtobufSchemaMetadata.metadataValue);
                        if (metadataKey.equals(DECIMAL_SCALE_VALUE)) {
                            try {
                                schemaBuilder = Decimal.builder(Integer.valueOf(metadataValue));
                                schemaBuilder.parameter(DECIMAL_SCALE_VALUE, metadataValue);
                                break;
                            } catch (NumberFormatException ex) {
                                // ignore
                            }
                        }
                    }
                    schemaBuilder = Decimal.builder(DECIMAL_DEFAULT_SCALE);
                    break;
                }

                String fullName = fieldDescriptor.getMessageType().getFullName();
                schemaBuilder = SchemaBuilder.struct().name(fullName);
                processFieldDescriptors(schemaBuilder, fieldDescriptor.getMessageType().getFields());
                break;
            }
            default:
                throw new DataException("Invalid Protobuf type passed: " + protobufType);
        }

        //Protobuf provides different types of integers.
        //We add metadata to Connect schema to store the original type used.
        if (TYPES_TO_ADD_METADATA.contains(protobufType)) {
            schemaBuilder.parameter(PROTOBUF_TYPE, protobufType.name().toUpperCase());
        }

        if (protobufType.equals(ENUM)) { //ENUM case; storing ENUM data as metadata to avoid being lost in translation.
            schemaBuilder.parameter(PROTOBUF_TYPE, PROTOBUF_ENUM_TYPE);
            // iterating through the values of the Enum to store each one
            for (Descriptors.EnumValueDescriptor enumValueDescriptor: fieldDescriptor.getEnumType().getValues()) {
                schemaBuilder.parameter(PROTOBUF_ENUM_VALUE + enumValueDescriptor.getName(),
                        String.valueOf(enumValueDescriptor.getNumber()));
            }
            schemaBuilder.parameter(PROTOBUF_ENUM_NAME, fieldDescriptor.getEnumType().getFullName());
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
