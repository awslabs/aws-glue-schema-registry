package com.amazonaws.services.schemaregistry;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.collect.ImmutableMap;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.WordFactory;

import java.util.Map;

import static com.amazonaws.services.schemaregistry.ByteArrayConverter.fromCReadOnlyByteArray;
import static com.amazonaws.services.schemaregistry.ByteArrayConverter.toCMutableByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.C_GlueSchemaRegistryErrorPointerHolder;
import static com.amazonaws.services.schemaregistry.DataTypes.C_GlueSchemaRegistrySchema;
import static com.amazonaws.services.schemaregistry.DataTypes.C_MutableByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.C_ReadOnlyByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.HandlerDirectives;
import static com.amazonaws.services.schemaregistry.DataTypes.newGlueSchemaRegistrySchema;

/**
 * Entry point class for the serialization methods of GSR shared library.
 */
@CContext(HandlerDirectives.class)
public class GlueSchemaRegistryDeserializationHandler {

    @CEntryPoint(name = "initialize_deserializer")
    public static void initializeDeserializer(IsolateThread isolateThread) {
        //TODO: Add GlueSchemaRegistryConfiguration to this method. This is hard-coded for now.
        //TODO: Error handling
        Map<String, String> configMap =
            ImmutableMap.of(
                AWSSchemaRegistryConstants.AWS_REGION,
                "us-east-1"
            );
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration =
            new GlueSchemaRegistryConfiguration(configMap);

        DeserializerInstance.create(glueSchemaRegistryConfiguration);
    }

    @CEntryPoint(name = "decode")
    public static C_MutableByteArray decode(
        IsolateThread isolateThread,
        C_ReadOnlyByteArray c_readOnlyByteArray,
        C_GlueSchemaRegistryErrorPointerHolder errorPointer) {

        try {
            byte[] bytesToDecode = fromCReadOnlyByteArray(c_readOnlyByteArray);

            //Assuming deserializer instance is already initialized
            GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer = DeserializerInstance.get();

            byte[] decodedBytes =
                glueSchemaRegistryDeserializer.getData(bytesToDecode);

            return toCMutableByteArray(decodedBytes, errorPointer);
        } catch (Exception | Error e) {
            ExceptionWriter.write(errorPointer, e);
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "decode_schema")
    public static C_GlueSchemaRegistrySchema decodeSchema(
        IsolateThread isolateThread,
        C_ReadOnlyByteArray c_readOnlyByteArray,
        C_GlueSchemaRegistryErrorPointerHolder errorPointer) {

        try {
            byte[] bytesToDecode = fromCReadOnlyByteArray(c_readOnlyByteArray);

            //Assuming serializer instance is already initialized
            GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer = DeserializerInstance.get();
            Schema decodedSchema =
                glueSchemaRegistryDeserializer.getSchema(bytesToDecode);

            CTypeConversion.CCharPointerHolder cSchemaNamePointer =
                CTypeConversion.toCString(decodedSchema.getSchemaName());
            CTypeConversion.CCharPointerHolder cSchemaDefPointer =
                CTypeConversion.toCString(decodedSchema.getSchemaDefinition());
            CTypeConversion.CCharPointerHolder cDataFormatPointer =
                CTypeConversion.toCString(decodedSchema.getDataFormat());

            //TODO: We can potentially expose the C Strings to target language layer to
            //prevent copying strings repeatedly.
            C_GlueSchemaRegistrySchema c_glueSchemaRegistrySchema = newGlueSchemaRegistrySchema(
                cSchemaNamePointer.get(),
                cSchemaDefPointer.get(),
                cDataFormatPointer.get(),
                errorPointer
            );
            //newGlueSchemaRegistrySchema has it's own copy of these attributes.
            cDataFormatPointer.close();
            cSchemaDefPointer.close();
            cSchemaNamePointer.close();

            return c_glueSchemaRegistrySchema;
        } catch (Exception | Error e) {
            ExceptionWriter.write(errorPointer, e);
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "can_decode")
    public static byte canDecode(
        IsolateThread isolateThread,
        C_ReadOnlyByteArray c_readOnlyByteArray,
        C_GlueSchemaRegistryErrorPointerHolder errorPointer) {
        try {

            byte[] bytesToDecode = fromCReadOnlyByteArray(c_readOnlyByteArray);

            GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer = DeserializerInstance.get();
            boolean canDeserialize =
                glueSchemaRegistryDeserializer.canDeserialize(bytesToDecode);

            if (errorPointer.isNonNull()) {
                errorPointer.write(WordFactory.nullPointer());
            }
            return CTypeConversion.toCBoolean(canDeserialize);
        } catch (Exception | Error e) {
            ExceptionWriter.write(errorPointer, e);
            return CTypeConversion.toCBoolean(false);
        }
    }
}
