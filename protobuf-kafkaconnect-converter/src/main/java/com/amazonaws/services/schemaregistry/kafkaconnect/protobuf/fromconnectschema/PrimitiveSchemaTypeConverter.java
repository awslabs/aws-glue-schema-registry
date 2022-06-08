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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import metadata.ProtobufSchemaMetadata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_INT16;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_INT8;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.METADATA_IMPORT;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64;

/**
 * Converts Primitive Connect schema types to Protobuf primitive types.
 */
public class PrimitiveSchemaTypeConverter implements SchemaTypeConverter {

    private static final Map<Schema.Type, DescriptorProtos.FieldDescriptorProto.Type>
        CONNECT_PROTO_CONVERSION_MAP = ImmutableMap.<Schema.Type, DescriptorProtos.FieldDescriptorProto.Type>builder()
        .put(Schema.Type.INT8, TYPE_INT32)
        .put(Schema.Type.INT16, TYPE_INT32)
        .put(Schema.Type.INT32, TYPE_INT32)
        .put(Schema.Type.INT64, TYPE_INT64)
        .put(Schema.Type.FLOAT32, TYPE_FLOAT)
        .put(Schema.Type.FLOAT64, TYPE_DOUBLE)
        .put(Schema.Type.BOOLEAN, TYPE_BOOL)
        .put(Schema.Type.BYTES, TYPE_BYTES)
        .put(Schema.Type.STRING, TYPE_STRING)
        .build();

    private static final Map<DescriptorProtos.FieldDescriptorProto.Type, List<DescriptorProtos.FieldDescriptorProto.Type>>
        CONNECT_METADATA_TYPE_CONVERSION_MAP = ImmutableMap.of(
        TYPE_INT32, Arrays.asList(TYPE_SINT32, TYPE_SFIXED32, TYPE_INT32),
        TYPE_INT64,
        Arrays.asList(TYPE_SINT64, TYPE_UINT64, TYPE_SFIXED64, TYPE_FIXED64, TYPE_FIXED32, TYPE_UINT32, TYPE_INT64)
    );

    private DescriptorProtos.FieldDescriptorProto.Type getProtobufType(final Schema schema) {
        final Schema.Type schemaType = schema.type();
        if (!CONNECT_PROTO_CONVERSION_MAP.containsKey(schemaType)) {
            throw new IllegalStateException(
                "Invalid connect type passed to Primitive type converter: " + schemaType);
        }

        DescriptorProtos.FieldDescriptorProto.Type type = CONNECT_PROTO_CONVERSION_MAP.get(schemaType);
        final Map<String, String> schemaParams = schema.parameters();

        if (schemaParams == null || !schemaParams.containsKey(PROTOBUF_TYPE) || !CONNECT_METADATA_TYPE_CONVERSION_MAP
            .containsKey(type)) {
            return type;
        }

        //Map to any valid specified protobuf types in the metadata.
        final DescriptorProtos.FieldDescriptorProto.Type specifiedProtobufType =
            DescriptorProtos.FieldDescriptorProto.Type.valueOf("TYPE_" + schemaParams.get(PROTOBUF_TYPE).toUpperCase());

        if (!CONNECT_METADATA_TYPE_CONVERSION_MAP.get(type).contains(specifiedProtobufType)) {
            throw new DataException(
                String.format("Protobuf type for %s is specified to use %s which is not allowed", type,
                    specifiedProtobufType)
            );
        }

        return specifiedProtobufType;
    }

    private void setMetadataOptions(DescriptorProtos.FieldDescriptorProto.Builder builder,
        DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder, String metadataKey,
        String metadataValue) {

        addImportToProtobufSchema(fileDescriptorProtoBuilder, METADATA_IMPORT);

        DescriptorProtos.FieldOptions.Builder keyOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder();
        keyOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataKey, metadataKey);
        builder.mergeOptions(keyOptionsBuilder.build());

        DescriptorProtos.FieldOptions.Builder valueOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder();
        valueOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataValue, metadataValue);
        builder.mergeOptions(valueOptionsBuilder.build());
    }

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
        final Schema schema, final DescriptorProtos.DescriptorProto.Builder descriptorProto,
        final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        DescriptorProtos.FieldDescriptorProto.Builder builder = DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setType(getProtobufType(schema))
                //Label is OPTIONAL as this is a simple primitive type.
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);

        if (schema.type().equals(Schema.Type.INT8)) {
            setMetadataOptions(builder, fileDescriptorProtoBuilder, CONNECT_SCHEMA_TYPE, CONNECT_SCHEMA_INT8);
        } else if (schema.type().equals(Schema.Type.INT16)) {
            setMetadataOptions(builder, fileDescriptorProtoBuilder, CONNECT_SCHEMA_TYPE, CONNECT_SCHEMA_INT16);
        }

        return builder;
    }
}
