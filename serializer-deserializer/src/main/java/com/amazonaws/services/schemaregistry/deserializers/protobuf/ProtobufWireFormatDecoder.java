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

import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.UncheckedIOException;

@RequiredArgsConstructor
public class ProtobufWireFormatDecoder {
    private final MessageIndexFinder messageIndexFinder;

    public Object decode(@NonNull byte[] data, @NonNull Descriptors.FileDescriptor descriptor,
                         ProtobufMessageType messageType) throws IOException {
        final CodedInputStream codedInputStream = CodedInputStream.newInstance(data);
        int messageIndex;
        Descriptors.Descriptor messageDescriptor;
        try {
            messageIndex = codedInputStream.readUInt32();
            if (messageIndex < 0) {
                throw new IllegalStateException("Message index cannot be negative: " + messageIndex);
            }
            messageDescriptor = messageIndexFinder.getByIndex(descriptor, messageIndex);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        switch (messageType) {
            case POJO:
                return null;
            case DYNAMIC_MESSAGE:
            default:
                return decodeDynamicMessage(messageDescriptor, codedInputStream);
        }
    }

    /**
     * Deserialization method for DynamicMessage and Unknown ProtobufMessageTypes
     * @param descriptor
     * @param data
     * @return DynamicMessage created from the parameters
     * @throws IOException
     */
    private DynamicMessage decodeDynamicMessage(Descriptors.Descriptor descriptor, CodedInputStream data)
            throws IOException {
        return DynamicMessage.parseFrom(descriptor, data);
    }
}
