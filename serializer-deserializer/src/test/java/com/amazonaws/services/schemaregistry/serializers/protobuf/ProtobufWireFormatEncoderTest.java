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

import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufWireFormatEncoderTest {
    private static final Integer MESSAGE_INDEX = 1;

    private static final ProtobufWireFormatEncoder PROTOBUF_WIRE_FORMAT =
        new ProtobufWireFormatEncoder(new MessageIndexFinder());

    private static final String NAME = "Foo";
    private static final String NAME_FIELD = "name";
    private static final Basic.Customer CUSTOMER_MESSAGE = Basic.Customer.newBuilder().setName(NAME).build();
    private static final Descriptors.FileDescriptor CUSTOMER_FILE_DESCRIPTOR =
        Basic.Customer.getDescriptor().getFile();
    private static final DynamicMessage DYNAMIC_CUSTOMER_MESSAGE =
        DynamicMessage.newBuilder(Basic.Customer.getDescriptor())
            .setField(Basic.Customer.getDescriptor().findFieldByName(NAME_FIELD), NAME)
            .build();

    @Test
    public void testEncode_WhenNullsArePassed_ThrowsException() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PROTOBUF_WIRE_FORMAT.encode(null, CUSTOMER_FILE_DESCRIPTOR)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> PROTOBUF_WIRE_FORMAT.encode(CUSTOMER_MESSAGE, null)
        );
    }

    @Test
    public void testPrefixMessageIndexToBytes_WhenNullsArePassed_ThrowsException() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PROTOBUF_WIRE_FORMAT.prefixMessageIndexToBytes(null, CUSTOMER_FILE_DESCRIPTOR, CUSTOMER_MESSAGE.getDescriptorForType())
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> PROTOBUF_WIRE_FORMAT.prefixMessageIndexToBytes(new byte[]{}, null, CUSTOMER_MESSAGE.getDescriptorForType())
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> PROTOBUF_WIRE_FORMAT.prefixMessageIndexToBytes(new byte[]{}, CUSTOMER_FILE_DESCRIPTOR, null)
        );
    }

    @ParameterizedTest
    @MethodSource("testMessageProvider")
    public void testEncode_EncodesMessageAndMessageIndex_SuccessfullyDecodesToPOJO(Message message) throws Exception {
        byte[] encodedMessage = PROTOBUF_WIRE_FORMAT.encode(message, CUSTOMER_FILE_DESCRIPTOR);

        CodedInputStream codedInputStream = CodedInputStream.newInstance(encodedMessage);

        int actualMessageIndex = codedInputStream.readUInt32();

        assertEquals(MESSAGE_INDEX, actualMessageIndex);

        Basic.Customer actualCustomerMessage = Basic.Customer.parseFrom(codedInputStream);

        assertEquals(CUSTOMER_MESSAGE, actualCustomerMessage);
    }

    @ParameterizedTest
    @MethodSource("testMessageProvider")
    public void testEncode_EncodesMessageAndMessageIndex_SuccessfullyDecodesToDynamicMessage(Message message) throws Exception {
        byte[] encodedMessage = PROTOBUF_WIRE_FORMAT.encode(message, CUSTOMER_FILE_DESCRIPTOR);

        CodedInputStream codedInputStream = CodedInputStream.newInstance(encodedMessage);

        int actualMessageIndex = codedInputStream.readUInt32();

        assertEquals(MESSAGE_INDEX, actualMessageIndex);

        DynamicMessage actualDynamicCustomerMessage =
            DynamicMessage.parseFrom(Basic.Customer.getDescriptor(), codedInputStream);

        assertEquals(DYNAMIC_CUSTOMER_MESSAGE, actualDynamicCustomerMessage);
    }

    @ParameterizedTest
    @MethodSource("testMessageProvider")
    public void testPrefixMessageIndexToBytes_SuccessfullyPrefixesCorrectMessageIndex(Message message)
        throws Exception {
        byte[] messageBytes = message.toByteArray();
        byte[] prefixedBytes = PROTOBUF_WIRE_FORMAT.prefixMessageIndexToBytes(messageBytes, CUSTOMER_FILE_DESCRIPTOR,
            message.getDescriptorForType());
        CodedInputStream codedInputStream = CodedInputStream.newInstance(prefixedBytes);

        int actualMessageIndex = codedInputStream.readUInt32();

        assertEquals(MESSAGE_INDEX, actualMessageIndex);
    }

    private static Stream<Arguments> testMessageProvider() {
        return Stream.of(CUSTOMER_MESSAGE, DYNAMIC_CUSTOMER_MESSAGE).map(Arguments::of);
    }
}