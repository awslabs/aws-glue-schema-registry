/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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
package com.amazonaws.services.schemaregistry.deserializers.protobuf;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class ProtobufDeserializer implements GlueSchemaRegistryDataFormatDeserializer {
    private static final GlueSchemaRegistryDeserializerDataParser DESERIALIZER_DATA_PARSER =
            GlueSchemaRegistryDeserializerDataParser.getInstance();
    private final ProtobufWireFormatDecoder protoDecoder;
    private final ProtobufMessageType protobufMessageType;

    @Builder
    public ProtobufDeserializer(GlueSchemaRegistryConfiguration configs) {
        this.protoDecoder = new ProtobufWireFormatDecoder(new MessageIndexFinder());
        this.protobufMessageType = configs.getProtobufMessageType();
    }

    @Override
    public Object deserialize(@NonNull ByteBuffer buffer, @NonNull Schema schema) {
        try {
            String schemaString = schema.getSchemaDefinition();
            String schemaName = schema.getSchemaName();
            ProtoFileElement fileElement = ProtoParser.Companion.parse(FileDescriptorUtils.DEFAULT_LOCATION, schemaString);
            Descriptors.FileDescriptor fileDescriptor = FileDescriptorUtils.protoFileToFileDescriptor(fileElement);

            byte[] data = DESERIALIZER_DATA_PARSER.getPlainData(buffer);

            return protoDecoder.decode(data, fileDescriptor, protobufMessageType, schemaName);
        } catch (Exception e) {
            throw new AWSSchemaRegistryException("Exception occurred while de-serializing Protobuf message", e);
        }
    }
}