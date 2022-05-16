package com.amazonaws.services.schemaregistry;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.collect.ImmutableMap;
import com.oracle.svm.core.c.CConst;
import com.oracle.svm.core.c.ProjectHeaderFile;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.function.CFunction;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.PointerBase;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Entry point class for the serialization methods of GSR shared library.
 */
@CContext(GlueSchemaRegistrySerializationHandler.HandlerDirectives.class)
public class GlueSchemaRegistrySerializationHandler {

    /**
     * Declare headers required by the shared library.
     */
    static class HandlerDirectives implements CContext.Directives {

        public static final String INCLUDE_PATH = "c/include/";

        @Override
        public List<String> getHeaderFiles() {
            return Stream.of(
                "glue_schema_registry_schema.h",
                "read_only_byte_array.h",
                "mutable_byte_array.h"
            )
                .map(header -> ProjectHeaderFile.resolve("", INCLUDE_PATH + header))
                .collect(Collectors.toList());
        }
    }

    /**
     * Define the functions to import from C headers.
     */
    @CFunction("new_glue_schema_registry_schema")
    protected static native C_GlueSchemaRegistrySchema
    newGlueSchemaRegistrySchema(CCharPointer schemaName, CCharPointer schemaDef, CCharPointer dataFormat);

    @CStruct("glue_schema_registry_schema")
    public interface C_GlueSchemaRegistrySchema extends PointerBase {

        //Read access of a field. A call to the function is replaced with a raw memory load.
        @CField("schema_name")
        CCharPointer getSchemaName();

        @CField("schema_def")
        CCharPointer getSchemaDef();

        @CField("data_format")
        CCharPointer getDataFormat();
    }

    @CStruct("read_only_byte_array")
    public interface C_ReadOnlyByteArray extends PointerBase {

        @CField("data")
        PointerBase getData();

        @CField("len")
        long getLen();
    }

    @CStruct("mutable_byte_array")
    public interface C_MutableByteArray extends PointerBase {

        @CField("data")
        PointerBase getData();

        @CField("max_len")
        long getMaxLen();
    }

    @CFunction("new_mutable_byte_array")
    protected static native C_MutableByteArray newMutableByteArray(long maxLen);

    @CFunction("mutable_byte_array_write")
    protected static native void writeToMutableArray(C_MutableByteArray array, long index, byte b);

    @CEntryPoint(name = "initialize_serializer")
    public static void initializeSerializer(IsolateThread isolateThread) {
        //TODO: Add GlueSchemaRegistryConfiguration to this method. This is hard-coded for now.
        //TODO: Error handling
        Map<String, String> configMap =
            ImmutableMap.of(
                AWSSchemaRegistryConstants.AWS_REGION,
                "us-east-1",
                AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
                "true"
            );
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration =
            new GlueSchemaRegistryConfiguration(configMap);

        SerializerInstance.create(glueSchemaRegistryConfiguration);
    }

    @CEntryPoint(name = "encode_with_schema")
    public static C_MutableByteArray encodeWithSchema(
        IsolateThread isolateThread, C_ReadOnlyByteArray c_readOnlyByteArray, @CConst CCharPointer c_transportName, C_GlueSchemaRegistrySchema c_glueSchemaRegistrySchema) {
        //Access the input C schema object
        final String schemaName = CTypeConversion.toJavaString(c_glueSchemaRegistrySchema.getSchemaName());
        final String schemaDef = CTypeConversion.toJavaString(c_glueSchemaRegistrySchema.getSchemaDef());
        final String dataFormat = CTypeConversion.toJavaString(c_glueSchemaRegistrySchema.getDataFormat());
        final String transportName = CTypeConversion.toJavaString(c_transportName);

        Schema javaSchema = new Schema(schemaDef, dataFormat, schemaName);

        //Read the c_byteArray data and create a new mutable byte array with encoded data
        PointerBase cData = c_readOnlyByteArray.getData();
        //This is validated to fit in Integer limits.
        int cDataLen = Math.toIntExact(c_readOnlyByteArray.getLen());
        //Copy the bytebuffer to a byte [].
        //TODO: This won't be needed if Java APIs accepted ByteBuffer instead of byte[]
        ByteBuffer javaByteBuffer = CTypeConversion.asByteBuffer(cData, cDataLen);
        byte [] bytesToEncode = new byte[cDataLen];
        javaByteBuffer.get(bytesToEncode);

        //Assuming serializer instance is already initialized
        GlueSchemaRegistrySerializer glueSchemaRegistrySerializer = SerializerInstance.get();
        byte[] encodedBytes =
            glueSchemaRegistrySerializer.encode(transportName, javaSchema, bytesToEncode);

        int encodedByteArrayLen = encodedBytes.length;
        //TODO: Handle errors if newMutableByteArray is null in case of limits.
        C_MutableByteArray mutableByteArray = newMutableByteArray(encodedByteArrayLen);

        //TODO: Check for performance issues with this.
        for (int index = 0 ; index < encodedByteArrayLen; index++) {
            writeToMutableArray(mutableByteArray, index, encodedBytes[index]);
        }

        return mutableByteArray;
    }
}
