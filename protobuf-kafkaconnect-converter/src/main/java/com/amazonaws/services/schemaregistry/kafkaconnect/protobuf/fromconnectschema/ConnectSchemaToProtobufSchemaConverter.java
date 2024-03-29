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

import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;

/**
 * Converts Kafka Connect schemas to Protobuf3 schemas.
 */
public class ConnectSchemaToProtobufSchemaConverter {
    private static final String PACKAGE_NAME_PREFIX =
        "com.amazonaws.services.schemaregistry.kafkaconnect.autogenerated.";
    private static final String PROTO3_SYNTAX = "proto3";

    /**
     * Converts the given Kafka connect schema into a Protobuf FileDescriptor
     * that can be used to generate Protobuf objects.
     * @param schema Kafka Connect schemas.
     * @return FileDescriptor for the generated Protobuf Schema.
     */
    public Descriptors.FileDescriptor convert(@NonNull final Schema schema) {
        final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder =
            buildFileDescriptorProto(schema);

        return buildFileDescriptor(fileDescriptorProtoBuilder.build());
    }

    /**
     * Builds the file level information required for generating the Protobuf FileDescriptor.
     *
     * @param schema Kafka Connect schema.
     * @return FileDescriptorProtoBuilder
     */
    private DescriptorProtos.FileDescriptorProto.Builder buildFileDescriptorProto(final Schema schema) {
        final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder =
            DescriptorProtos.FileDescriptorProto.newBuilder();

        final String schemaName = schema.name();

        //Setting to a constant value. Revisit this during metadata storage to make it configurable.
        fileDescriptorProtoBuilder.setPackage(PACKAGE_NAME_PREFIX + schemaName);
        fileDescriptorProtoBuilder.setName(schemaName + ".proto");
        //We only support schema generation in Proto3 syntax for now.
        fileDescriptorProtoBuilder.setSyntax(PROTO3_SYNTAX);

        //Creating parent level struct that contains the parent level fields.
        final DescriptorProtos.DescriptorProto.Builder messageDescriptorProtoBuilder =
            buildMessageDescriptorProto(schema, fileDescriptorProtoBuilder);
        fileDescriptorProtoBuilder.addMessageType(messageDescriptorProtoBuilder);

        return fileDescriptorProtoBuilder;
    }

    private DescriptorProtos.DescriptorProto.Builder buildMessageDescriptorProto(
        final Schema schema, final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        final DescriptorProtos.DescriptorProto.Builder messageDescriptorProtoBuilder =
            DescriptorProtos.DescriptorProto.newBuilder();

        //TODO: Revisit for compilation
        messageDescriptorProtoBuilder.setName(schema.name());

        FieldBuilder.build(schema, fileDescriptorProtoBuilder, messageDescriptorProtoBuilder);

        return messageDescriptorProtoBuilder;
    }

    @SneakyThrows
    private Descriptors.FileDescriptor buildFileDescriptor(DescriptorProtos.FileDescriptorProto fileDescriptorProto) {
        return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, FileDescriptorUtils.baseDependencies());
    }
}
