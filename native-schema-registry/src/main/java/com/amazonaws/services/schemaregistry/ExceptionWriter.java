package com.amazonaws.services.schemaregistry;

import org.graalvm.nativeimage.c.type.CTypeConversion;

import static com.amazonaws.services.schemaregistry.DataTypes.C_GlueSchemaRegistryErrorPointerHolder;
import static com.amazonaws.services.schemaregistry.DataTypes.throwError;

/**
 * Writes the Exception / Error to given exception pointer holder.
 */
public class ExceptionWriter {
    //Code for ERR_CODE_RUNTIME_ERROR
    //TODO: Use the variable from header file.
    private static final int FAILURE_CODE = 5005;

    public static void write(C_GlueSchemaRegistryErrorPointerHolder errorPointer, Error error) {
        writeException(error.getMessage(), errorPointer);
    }

    public static void write(C_GlueSchemaRegistryErrorPointerHolder errorPointer, Throwable throwable) {
        writeException(throwable.getMessage(), errorPointer);
    }

    private static void writeException(String message, C_GlueSchemaRegistryErrorPointerHolder errorPointer) {

        final String errorMessage = message == null ? "" : message;

        CTypeConversion.CCharPointerHolder errorMessageAsCString = CTypeConversion.toCString(errorMessage);

        throwError(errorPointer, errorMessageAsCString.get(), FAILURE_CODE);
        errorMessageAsCString.close();
    }
}
