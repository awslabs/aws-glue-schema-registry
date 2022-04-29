package com.amazonaws.services.schemaregistry;

import com.amazonaws.services.schemaregistry.common.Schema;
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

import java.util.List;
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

        public static final String INCLUDE_PATH = "clang/include/";

        @Override
        public List<String> getHeaderFiles() {
            return Stream.of(
                "glue_schema_registry_schema.h"
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

    @CEntryPoint(name = "encode_with_schema")
    public static C_GlueSchemaRegistrySchema encodeWithSchema(
        IsolateThread isolateThread, C_GlueSchemaRegistrySchema c_glueSchemaRegistrySchema) {
        //Access the input C schema object
        final String schemaName = CTypeConversion.toJavaString(c_glueSchemaRegistrySchema.getSchemaName());
        final String schemaDef = CTypeConversion.toJavaString(c_glueSchemaRegistrySchema.getSchemaDef());
        final String dataFormat = CTypeConversion.toJavaString(c_glueSchemaRegistrySchema.getDataFormat());

        Schema javaSchema = new Schema(schemaDef, dataFormat, schemaName);

        System.out.println(
            String.format("Created Java Schema object: %s %s %s",
                javaSchema.getSchemaName(), javaSchema.getSchemaDefinition(), javaSchema.getDataFormat()));

        C_GlueSchemaRegistrySchema cGsrSchema = newGlueSchemaRegistrySchema(
            CTypeConversion.toCString(javaSchema.getSchemaName()).get(),
            CTypeConversion.toCString(javaSchema.getSchemaDefinition()).get(),
            CTypeConversion.toCString(javaSchema.getDataFormat()).get()
        );

        return cGsrSchema;
    }
}
