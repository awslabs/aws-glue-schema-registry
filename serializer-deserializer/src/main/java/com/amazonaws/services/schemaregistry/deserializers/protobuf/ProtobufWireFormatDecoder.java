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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.lang.reflect.Method;

public class ProtobufWireFormatDecoder {
    private final MessageIndexFinder messageIndexFinder;

    /**
     * Configuration supplying the POJO-mode class-name allowlist, or {@code null} when the
     * decoder is constructed without one. A {@code null} configuration applies no allowlist,
     * leaving the unconditional {@link Message} check in
     * {@link #deserializeToPojo(Descriptors.Descriptor, CodedInputStream)} as the only bound.
     */
    private final GlueSchemaRegistryConfiguration configs;

    public ProtobufWireFormatDecoder(MessageIndexFinder messageIndexFinder) {
        this(messageIndexFinder, null);
    }

    public ProtobufWireFormatDecoder(MessageIndexFinder messageIndexFinder,
                                     GlueSchemaRegistryConfiguration configs) {
        this.messageIndexFinder = messageIndexFinder;
        this.configs = configs;
    }

    public static Pair<Integer, CodedInputStream> getAndRemoveMessageIndex(byte[] data) throws IOException {
        final CodedInputStream codedInputStream = CodedInputStream.newInstance(data);
        final int messageIndex = codedInputStream.readUInt32();
        return Pair.of(messageIndex, codedInputStream);
    }

    public Object decode(@NonNull byte[] data, @NonNull Descriptors.FileDescriptor descriptor,
                         ProtobufMessageType messageType) throws IOException {
        Pair<Integer, CodedInputStream> indexAndStreamPair = getAndRemoveMessageIndex(data);

        final Descriptors.Descriptor messageDescriptor = messageIndexFinder.getByIndex(descriptor, indexAndStreamPair.getLeft());

        if (ProtobufMessageType.POJO.equals(messageType)) {
            return deserializeToPojo(messageDescriptor, indexAndStreamPair.getRight());
        } else {
            //Defaults to DynamicMessage if not set or set explicitly to DYNAMIC_MESSAGE.
            return deserializeToDynamicMessage(messageDescriptor, indexAndStreamPair.getRight());
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
     * <p>
     * The class name is derived from the schema definition, which is registry content rather
     * than application configuration: {@link ProtobufClassName#from(Descriptors.Descriptor)}
     * builds it from the schema's {@code java_package}, {@code java_outer_classname} and
     * message names. A schema therefore selects which class is loaded, so the resolved class
     * is checked before it is initialized or invoked.
     *
     * @param descriptor Descriptor associated with the message.
     * @param codedInputStream codedInputStream to read data from.
     * @return deserialized POJO
     */
    private Object deserializeToPojo(final Descriptors.Descriptor descriptor, final CodedInputStream codedInputStream) {
        final String className = ProtobufClassName.from(descriptor);

        if (configs != null && !configs.isProtobufClassNameAllowed(className)) {
            throw new RuntimeException(String.format(
                "Protobuf schema resolves to class %s, which is not permitted by %s.",
                className, AWSSchemaRegistryConstants.PROTOBUF_CLASS_NAME_ALLOWLIST));
        }

        final Class<?> classType;
        try {
            // initialize=false: the single-argument Class.forName runs the target's static
            // initializer as a side effect of loading it, which would let a schema trigger code
            // in any class on the classpath before any check below has run. Deferring
            // initialization means a rejected class is never initialized. The classloader is the
            // one that loaded this class, which is what Class.forName(String) would have used.
            classType = Class.forName(className, false, ProtobufWireFormatDecoder.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            final String errorMsg = String.format("Error de-serializing data into Message class: %s", className);
            throw new RuntimeException(errorMsg, e);
        }

        // Every class this path is meant to reach is generated by protoc and so implements
        // Message. Requiring it keeps a schema from naming an unrelated class, without needing
        // the consumer to configure anything.
        if (!Message.class.isAssignableFrom(classType)) {
            throw new RuntimeException(String.format(
                "Protobuf schema resolves to class %s, which is not a %s.",
                className, Message.class.getName()));
        }

        try {
            final Method parseMethod = classType.getMethod("parseFrom", CodedInputStream.class);
            return parseMethod.invoke(classType, codedInputStream);
        } catch (Exception e) {
            final String errorMsg = String.format("Error de-serializing data into Message class: %s", className);
            throw new RuntimeException(errorMsg, e);
        }
    }
}
