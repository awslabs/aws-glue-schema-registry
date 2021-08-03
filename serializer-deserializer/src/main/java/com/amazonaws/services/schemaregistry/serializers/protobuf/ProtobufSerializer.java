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
package com.amazonaws.services.schemaregistry.serializers.protobuf;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Protobuf serialization helper.
 * This class is instantiated by GlueSchemaRegistryFacade to serialize Protobuf-type objects
 *
 */
@Slf4j
public class ProtobufSerializer implements GlueSchemaRegistryDataFormatSerializer {
    private GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs;
    private ProtobufWireFormatEncoder protoEncoder;

    /**
     * Constructor
     *
     * @param configs configuration elements
     */
    @Builder
    public ProtobufSerializer(GlueSchemaRegistryConfiguration configs) {
        this.schemaRegistrySerDeConfigs = configs;
        this.protoEncoder = new ProtobufWireFormatEncoder(new MessageIndexFinder());
    }

    /**
     * Serialize the Protobuf object to bytes
     *
     * @param data the Protobuf object for serialization
     * @return the serialized byte array
     * @throws AWSSchemaRegistryException AWS Schema Registry Exception
     */
    @Override
    public byte[] serialize(@NonNull Object data) {
        try {
            validate(data);
            Message protobufMessage = (Message) data;
            return protoEncoder.encode(protobufMessage, protobufMessage.getDescriptorForType().getFile());
        } catch (Exception e) {
            throw new AWSSchemaRegistryException(
                    "Could not serialize from the type provided", e);
        }
    }

    /**
     * Get the schema definition.
     *
     * @param object object for which schema definition has to be derived
     * @return schema string
     */
    @Override
    public String getSchemaDefinition(@NonNull Object object) {
        try {
            validate(object);
            Message message = (Message) object;
            Descriptors.FileDescriptor fileDescriptor = message.getDescriptorForType().getFile();
            DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptor.toProto();
            ProtoFileElement schemaElement = FileDescriptorUtils.fileDescriptorToProtoFile(fileDescriptorProto);
            return schemaElement.toSchema();
        } catch (Exception e) {
            throw new AWSSchemaRegistryException(
                    "Could not generate schema from the type provided", e);
        }
    }

    @Override
    public void validate(@NonNull String schemaDefinition, @NonNull byte[] data) {
        //TODO: Implement
        //Left blank as the schema string representation has not been solidified
    }

    @Override
    public void validate(@NonNull Object object) {
        if (!(object instanceof Message)) {
            throw new AWSSchemaRegistryException(
                    "Object is not of Message type: " + object.getClass());
        }
    }
}
