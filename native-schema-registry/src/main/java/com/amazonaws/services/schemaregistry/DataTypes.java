package com.amazonaws.services.schemaregistry;

import com.oracle.svm.core.c.ProjectHeaderFile;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CFunction;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.word.PointerBase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CContext(DataTypes.HandlerDirectives.class)
public class DataTypes {
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
     * Define the data types and functions to import from C headers.
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
    public static native C_MutableByteArray newMutableByteArray(long maxLen);

    @CFunction("mutable_byte_array_write")
    public static native void writeToMutableArray(C_MutableByteArray array, long index, byte b);

}
