package com.amazonaws.services.schemaregistry;

import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.PointerBase;

import java.nio.ByteBuffer;

import static com.amazonaws.services.schemaregistry.DataTypes.C_GlueSchemaRegistryErrorPointerHolder;
import static com.amazonaws.services.schemaregistry.DataTypes.C_MutableByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.C_ReadOnlyByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.newMutableByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.writeToMutableArray;

/**
 * Converts Java Byte arrays to/fro C byte arrays.
 */
public class ByteArrayConverter {
    public static byte[] fromCReadOnlyByteArray(C_ReadOnlyByteArray c_readOnlyByteArray) {
        PointerBase cData = c_readOnlyByteArray.getData();
        //This is validated to fit in Integer limits.
        int cDataLen = Math.toIntExact(c_readOnlyByteArray.getLen());

        //Copy the bytebuffer to a byte [].
        //TODO: This won't be needed if Java APIs accepted ByteBuffer instead of byte[]
        ByteBuffer javaByteBuffer = CTypeConversion.asByteBuffer(cData, cDataLen);
        byte[] bytes = new byte[cDataLen];
        javaByteBuffer.get(bytes);

        return bytes;
    }

    public static C_MutableByteArray toCMutableByteArray(
        byte[] bytes,
        C_GlueSchemaRegistryErrorPointerHolder errorPointerHolder) {
        int len = bytes.length;
        C_MutableByteArray mutableByteArray = newMutableByteArray(len, errorPointerHolder);

        //If error encountered creating a mutable_byte_array, return NULL with appropriate error set.
        if (mutableByteArray.isNull()) {
            return mutableByteArray;
        }

        //TODO: Check for performance issues with this.
        for (int index = 0; index < len; index++) {
            writeToMutableArray(mutableByteArray, index, bytes[index], errorPointerHolder);
        }
        return mutableByteArray;
    }
}
