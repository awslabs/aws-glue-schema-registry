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
            final byte[] data = DESERIALIZER_DATA_PARSER.getPlainData(buffer);

            final String schemaDefinition = schema.getSchemaDefinition();
            final String schemaName = schema.getSchemaName();
            final String protoFileName = getProtoFileName(schemaName);

            final Descriptors.FileDescriptor fileDescriptor =
                ProtobufSchemaParser.parse(schemaDefinition, protoFileName);

            return protoDecoder.decode(data, fileDescriptor, protobufMessageType);
        } catch (Exception e) {
            throw new AWSSchemaRegistryException("Exception occurred while de-serializing Protobuf message", e);
        }
    }

    /**
     * We use schemaName as protoFileName. During creation of schema, users are expected to define the schemaName same as
     * proto file name. They can optionally provide ".proto".
     * @param schemaName SchemaName registered with Glue Schema Registry service.
     * @return ProtoFileName string.
     */
    private String getProtoFileName(String schemaName) {
        final String protoExtension = ".proto";

        //If the schema name already contains ".proto" suffix, don't append the extension to protoFileName.
        final int extensionIndex = schemaName.lastIndexOf(protoExtension);

        //If extension is not present, append it.
        //Ex: Basic -> Basic.proto
        if (extensionIndex == -1) {
            return schemaName + protoExtension;
        }

        //If extension is at the end, return name as it is.
        //Ex: basic.proto -> basic.proto
        if (extensionIndex + protoExtension.length() == schemaName.length()) {
            return schemaName;
        }

        //If extension is not to the end, append it.
        //Ex: basic.protofoo.schema -> basic.protofoo.schema.proto
        return schemaName + protoExtension;
    }
}