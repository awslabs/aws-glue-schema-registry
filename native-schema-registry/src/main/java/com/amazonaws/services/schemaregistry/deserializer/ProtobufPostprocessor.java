package com.amazonaws.services.schemaregistry.deserializer;

import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufWireFormatDecoder;
import com.google.protobuf.CodedInputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProtobufPostprocessor {
    public static byte[] stripMessageIndex(byte[] data) throws IOException {
        Pair<Integer, CodedInputStream> indexAndStreamPair =
            ProtobufWireFormatDecoder.getAndRemoveMessageIndex(data);

        CodedInputStream inputStream = indexAndStreamPair.getRight();
        List<Byte> output = new ArrayList<>();
        while (!inputStream.isAtEnd()){

            byte b = inputStream.readRawByte();
            output.add(b);
        }
        Byte[] result = new Byte[output.size()];
        return ArrayUtils.toPrimitive(output.toArray(result));
    }
}
