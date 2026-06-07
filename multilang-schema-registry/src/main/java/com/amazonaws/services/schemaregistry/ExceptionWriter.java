package com.amazonaws.services.schemaregistry;

import org.graalvm.nativeimage.c.type.CTypeConversion;

import java.util.StringJoiner;

import static com.amazonaws.services.schemaregistry.DataTypes.C_GlueSchemaRegistryErrorPointerHolder;
import static com.amazonaws.services.schemaregistry.DataTypes.throwError;

/**
 * Writes the Exception / Error to given exception pointer holder.
 */
public class ExceptionWriter {
    //Code for ERR_CODE_RUNTIME_ERROR
    //TODO: Use the variable from header file.
    private static final int FAILURE_CODE = 5005;

    private static final int STACKTRACE_MESSAGE_LENGTH_LIMIT = 4096;

    public static void write(C_GlueSchemaRegistryErrorPointerHolder errorPointer, Error error) {
        writeException(getFullStackTraceMessage(error), errorPointer);
    }

    public static void write(C_GlueSchemaRegistryErrorPointerHolder errorPointer, Throwable throwable) {
        writeException(getFullStackTraceMessage(throwable), errorPointer);
    }

    private static void writeException(String message, C_GlueSchemaRegistryErrorPointerHolder errorPointer) {

        final String errorMessage = message == null ? "" : message;

        CTypeConversion.CCharPointerHolder errorMessageAsCString = CTypeConversion.toCString(errorMessage);

        throwError(errorPointer, errorMessageAsCString.get(), FAILURE_CODE);
        errorMessageAsCString.close();
    }
    /**
     * Captures the full stack trace message of a Throwable, so it can be observed in the target language layer,
     * For example, an exception thrown in Java layer will have these messages printed in target language layer:
     *
        Exception occurred while fetching or registering schema definition = {"type":"record","name":"User","namespace":"example.avro","fields":[{"name":"name","type":"string"},{"name":"favorite_number","type":["int","null"]},{"name":"favorite_color","type":["string","null"]}]}, schema name = test-topic-json
            com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher.getORRegisterSchemaVersionId(SchemaByDefinitionFetcher.java:99)
            com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(GlueSchemaRegistrySerializationFacade.java:86)
            com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade.encode(GlueSchemaRegistrySerializationFacade.java:125)
            com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl.encode(GlueSchemaRegistrySerializerImpl.java:79)
            com.amazonaws.services.schemaregistry.GlueSchemaRegistrySerializationHandler.encodeWithSchema(GlueSchemaRegistrySerializationHandler.java:90)
        at AWSGsrSerDe.GlueSchemaRegistrySerializer.Encode(String transportName, GlueSchemaRegistrySchema schema, Byte[] bytes) in /Users/haoqyang/Documents/gsr_multilang/aws-glue-schema-registry/multilang-schema-registry/csharp/AWSGsrSerDe/AWSGsrSerDe/GlueSchemaRegistrySerializer.cs:line 78
        at AWSGsrSerDe.serializer.GlueSchemaRegistryKafkaSerializer.Serialize(Object data, String topic) in /Users/haoqyang/Documents/gsr_multilang/aws-glue-schema-registry/multilang-schema-registry/csharp/AWSGsrSerDe/AWSGsrSerDe/serializer/GlueSchemaRegistryKafkaSerializer.cs:line 68
        at AWSGsrSerDe.Tests.serializer.GlueSchemaRegistryKafkaSerializerTests.KafkaSerDeTestForAvroGenericRecord() in /Users/haoqyang/Documents/gsr_multilang/aws-glue-schema-registry/multilang-schema-registry/csharp/AWSGsrSerDe/AWSGsrSerDe.Tests/serializer/GlueSchemaRegistryKafkaSerializerTests.cs:line 52
     */
    private static String getFullStackTraceMessage(Throwable throwable) {
        String throwableMessage;
        StringJoiner messageJoiner = new StringJoiner("\n");
        Boolean shouldAppendStackTrace = false;

        if (throwable.getStackTrace() != null) {
            shouldAppendStackTrace = true;
        }
        if (throwable == null) {
            throwableMessage = "NULL is thrown";
        } else if (throwable.getMessage() == null) {
            throwableMessage = throwable.getClass().getName() + " is thrown, but there is no detail message";
        } else {
            throwableMessage = throwable.getMessage();
        }
        messageJoiner.add(throwableMessage);

        if (shouldAppendStackTrace) {
            for (StackTraceElement stackTraceElement : throwable.getStackTrace()) {
                if (stackTraceElement == null) {
                    continue;
                } else {
                    messageJoiner.add(stackTraceElement.toString());
                }
            }
        }
        String message = messageJoiner.toString();
        message = message.substring(0, Math.min(message.length(), STACKTRACE_MESSAGE_LENGTH_LIMIT));
        return message;
    }
}
