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
import java.lang.reflect.Method;

@RequiredArgsConstructor
public class ProtobufWireFormatDecoder {
    private final MessageIndexFinder messageIndexFinder;

    public Object decode(@NonNull byte[] data, @NonNull Descriptors.FileDescriptor descriptor,
                         ProtobufMessageType messageType) throws IOException {
        final CodedInputStream codedInputStream = CodedInputStream.newInstance(data);
        final int messageIndex = codedInputStream.readUInt32();

        final Descriptors.Descriptor messageDescriptor = messageIndexFinder.getByIndex(descriptor, messageIndex);

        if (ProtobufMessageType.POJO.equals(messageType)) {
            return deserializeToPojo(messageDescriptor, codedInputStream);
        } else {
            //Defaults to DynamicMessage if not set or set explicitly to DYNAMIC_MESSAGE.
            return deserializeToDynamicMessage(messageDescriptor, codedInputStream);
        }
    }

    /**
     * Deserialization method for DynamicMessage ProtobufMessageType
     *
     * @param descriptor       Descriptor associated with the message.
     * @param codedInputStream codedInputStream to read the data from.
     * @return DynamicMessage created from the parameters
     */
    private DynamicMessage deserializeToDynamicMessage(final Descriptors.Descriptor descriptor,
        final CodedInputStream codedInputStream)
        throws IOException {
        return DynamicMessage.parseFrom(descriptor, codedInputStream);
    }

    /**
     * Deserialization method for POJO ProtobufMessageType.
     * Derives the class name from message descriptor and reflectively invokes it
     * to deserialize the bytes into POJO.
     *
     * @param descriptor Descriptor associated with the message.
     * @param codedInputStream codedInputStream to read data from.
     * @return deserialized POJO
     */
    private Object deserializeToPojo(final Descriptors.Descriptor descriptor, final CodedInputStream codedInputStream) {
        final String className = ProtobufClassName.from(descriptor);
        try {
            final Class<?> classType = Thread.currentThread().getContextClassLoader().loadClass(className);
            final Method parseMethod = classType.getMethod("parseFrom", CodedInputStream.class);
            return parseMethod.invoke(classType, codedInputStream);
        } catch (Exception e) {
            final String errorMsg = String.format("Error de-serializing data into Message class: %s", className);
            throw new RuntimeException(errorMsg, e);
        }
    }
}
