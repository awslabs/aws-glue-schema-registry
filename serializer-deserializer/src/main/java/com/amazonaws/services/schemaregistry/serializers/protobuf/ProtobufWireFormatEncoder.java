/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates.
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

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Writes the wire format for Schema Registry embedded Protobuf messages.
 */
@RequiredArgsConstructor
public class ProtobufWireFormatEncoder {
    private final MessageIndexFinder messageIndexFinder;

    /**
     * Encodes the message index as a zig-zag encoded variable size int into Byte stream.
     * @param message Protobuf message.
     * @param schemaFileDescriptor Protobuf schema file descriptor.
     * @return Encoded protobuf message with message index.
     */
    public byte[] encode(@NonNull Message message, @NonNull Descriptors.FileDescriptor schemaFileDescriptor) {
        final Descriptors.Descriptor descriptor = message.getDescriptorForType();
        try {
            return prefixMessageIndexToBytes(message.toByteArray(), schemaFileDescriptor, descriptor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public byte[] prefixMessageIndexToBytes(@NonNull byte[] bytesToEncode,
        @NonNull Descriptors.FileDescriptor schemaFileDescriptor, @NonNull Descriptors.Descriptor fileDescriptor)
        throws IOException {

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);

        final Integer messageIndex = messageIndexFinder.getByDescriptor(schemaFileDescriptor, fileDescriptor);
        codedOutputStream.writeUInt32NoTag(messageIndex);
        codedOutputStream.writeRawBytes(bytesToEncode);
        codedOutputStream.flush();

        return outputStream.toByteArray();
    }
}
