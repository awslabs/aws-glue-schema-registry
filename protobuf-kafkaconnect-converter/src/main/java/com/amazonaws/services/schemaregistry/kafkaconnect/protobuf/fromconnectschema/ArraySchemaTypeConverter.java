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

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName;

public class ArraySchemaTypeConverter implements SchemaTypeConverter {

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            final Schema schema, final DescriptorProtos.DescriptorProto.Builder descriptorProto,
            final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        final SchemaTypeConverter schemaTypeConverter = ConnectToProtobufTypeConverterFactory.get(schema.valueSchema());

        DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = schemaTypeConverter
                .toProtobufSchema(schema.valueSchema(), descriptorProto, fileDescriptorProtoBuilder);
        fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
        if (schema.valueSchema().type().equals(Schema.Type.STRUCT)) {
            fieldBuilder.setTypeName(getTypeName(schema.valueSchema().name()));
        }
        return fieldBuilder;
    }
}
