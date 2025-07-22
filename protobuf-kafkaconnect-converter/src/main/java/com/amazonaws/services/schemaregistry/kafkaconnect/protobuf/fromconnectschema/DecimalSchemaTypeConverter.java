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

import additionalTypes.Decimals;
import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import metadata.ProtobufSchemaMetadata;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_SCALE_VALUE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.METADATA_IMPORT;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;

public class DecimalSchemaTypeConverter implements SchemaTypeConverter  {
    private static final String DECIMAL_IMPORT = "additionalTypes/decimal.proto";

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            Schema schema, DescriptorProtos.DescriptorProto.Builder descriptorProto,
            DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        String typeName = getTypeName(Decimals.getDescriptor().getMessageTypes().get(0).getFullName());
        DescriptorProtos.FieldDescriptorProto.Builder builder = DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setType(TYPE_MESSAGE)
                .setTypeName(typeName)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);

        addImportToProtobufSchema(fileDescriptorProtoBuilder, DECIMAL_IMPORT);

        if (schema.parameters().containsKey(DECIMAL_SCALE_VALUE)) {
            addImportToProtobufSchema(fileDescriptorProtoBuilder, METADATA_IMPORT);
            DescriptorProtos.FieldOptions.Builder keyOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder();
            keyOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataKey, DECIMAL_SCALE_VALUE);
            builder.mergeOptions(keyOptionsBuilder.build());

            DescriptorProtos.FieldOptions.Builder valueOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder();
            valueOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataValue,
                    schema.parameters().get(DECIMAL_SCALE_VALUE));
            builder.mergeOptions(valueOptionsBuilder.build());
        }

        return builder;
    }
}
