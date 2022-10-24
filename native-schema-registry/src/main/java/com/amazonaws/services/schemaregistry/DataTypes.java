package com.amazonaws.services.schemaregistry;

import com.oracle.svm.core.c.ProjectHeaderFile;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CFunction;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CPointerTo;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.word.PointerBase;

import java.util.Collections;
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
        public static final String LIB_PATH = "target/";
        //Intentionally blank.
        public static final String PROJECT_NAME = "";

        @Override
        public List<String> getLibraries() {
            return Collections.singletonList("native_schema_registry_c_data_types");
        }

        @Override
        public List<String> getLibraryPaths() {
            String path = ProjectHeaderFile.resolve(PROJECT_NAME, LIB_PATH).replaceAll("\"", "");
            return Collections.singletonList(path);
        }

        @Override
        public List<String> getHeaderFiles() {
            return Stream.of(
                "glue_schema_registry_schema.h",
                "read_only_byte_array.h",
                "mutable_byte_array.h",
                "glue_schema_registry_error.h"
            )
                .map(header -> ProjectHeaderFile.resolve(PROJECT_NAME, INCLUDE_PATH + header))
                .collect(Collectors.toList());
        }
    }

    /**
     * Define the data types and functions to import from C headers.
     */
    @CFunction("new_glue_schema_registry_schema")
    public static native C_GlueSchemaRegistrySchema
    newGlueSchemaRegistrySchema(
        CCharPointer schemaName,
        CCharPointer schemaDef,
        CCharPointer dataFormat,
        C_GlueSchemaRegistryErrorPointerHolder errorPointerHolder
    );

    @CStruct("glue_schema_registry_schema")
    public interface C_GlueSchemaRegistrySchema extends PointerBase {

        //Read access of a field. A call to the function is replaced with a raw memory load.
        @CField("schema_name")
        CCharPointer getSchemaName();

        @CField("schema_def")
        CCharPointer getSchemaDef();

        @CField("data_format")
        CCharPointer getDataFormat();

        @CField("additional_schema_info")
        CCharPointer getAdditionalSchemaInfo();
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
    public static native C_MutableByteArray newMutableByteArray(
        long maxLen,
        C_GlueSchemaRegistryErrorPointerHolder errorPointerHolder
    );

    @CFunction("mutable_byte_array_write")
    public static native void writeToMutableArray(
        C_MutableByteArray array,
        long index,
        byte b,
        C_GlueSchemaRegistryErrorPointerHolder errorPointerHolder
    );

    @CStruct("glue_schema_registry_error")
    public interface C_GlueSchemaRegistryError extends PointerBase {

        //Read access of a field. A call to the function is replaced with a raw memory load.
        @CField("msg")
        CCharPointer getErrMsg();

        @CField("code")
        int getCode();
    }

    @CPointerTo(value = C_GlueSchemaRegistryError.class)
    public interface C_GlueSchemaRegistryErrorPointerHolder extends PointerBase {
        void write(C_GlueSchemaRegistryError value);
    }

    @CFunction("throw_error")
    public static native void throwError(C_GlueSchemaRegistryErrorPointerHolder pErr, CCharPointer msg, int code);
}
