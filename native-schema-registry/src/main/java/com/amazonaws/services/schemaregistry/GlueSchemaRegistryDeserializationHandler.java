package com.amazonaws.services.schemaregistry;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.config.NativeGlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.config.ConfigurationFileReader;
import com.amazonaws.services.schemaregistry.deserializer.ProtobufPostprocessor;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.WordFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Map;

import static com.amazonaws.services.schemaregistry.ByteArrayConverter.fromCReadOnlyByteArray;
import static com.amazonaws.services.schemaregistry.ByteArrayConverter.toCMutableByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.C_GlueSchemaRegistryErrorPointerHolder;
import static com.amazonaws.services.schemaregistry.DataTypes.C_GlueSchemaRegistrySchema;
import static com.amazonaws.services.schemaregistry.DataTypes.C_MutableByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.C_ReadOnlyByteArray;
import static com.amazonaws.services.schemaregistry.DataTypes.HandlerDirectives;
import static com.amazonaws.services.schemaregistry.DataTypes.newGlueSchemaRegistrySchema;
import static com.amazonaws.services.schemaregistry.config.NativeGlueSchemaRegistryConfiguration.USER_AGENT_APP_KEY;

/**
 * Entry point class for the serialization methods of GSR shared library.
 */
@CContext(HandlerDirectives.class)
@Slf4j
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
        NativeGlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new NativeGlueSchemaRegistryConfiguration(
                configMap);

        DeserializerInstance.create(glueSchemaRegistryConfiguration);
    }

    @CEntryPoint(name = "initialize_deserializer_with_config")
    public static int initializeDeserializerWithConfig(
        IsolateThread isolateThread,
        CCharPointer configFilePath,
        CCharPointer userAgentApp,
        C_GlueSchemaRegistryErrorPointerHolder errorPointer) {
        try {
            if (configFilePath.isNull()) {
                // Use default configuration when no config file provided
                initializeDeserializer(isolateThread);
                return 0;
            }

            final String filePath = CTypeConversion.toJavaString(configFilePath);
            final String userAgent = CTypeConversion.toJavaString(userAgentApp);
            final Map<String, String> configs = ConfigurationFileReader.loadConfigFromFile(filePath);
            configs.put(USER_AGENT_APP_KEY, userAgent);
            final NativeGlueSchemaRegistryConfiguration configuration =
                new NativeGlueSchemaRegistryConfiguration(configs);
            DeserializerInstance.create(configuration);

            return 0; // Success
        } catch (Exception | Error e) {
            log.error("Error while initializing deserializer with config", e);
            ExceptionWriter.write(errorPointer, e);
            return 1; // Error
        }
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

            //Get the schema and perform Protobuf post-processing if needed
            Schema decodedSchema =
                glueSchemaRegistryDeserializer.getSchema(bytesToDecode);
            if (DataFormat.PROTOBUF.name().equals(decodedSchema.getDataFormat())) {
                decodedBytes = ProtobufPostprocessor.stripMessageIndex(decodedBytes);
            }

            return toCMutableByteArray(decodedBytes, errorPointer);
        } catch (Exception | Error e) {
            log.error("Error while decoding data", e);
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
            log.error("Error while decoding schema", e);
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
            log.error("Error while can_decode ", e);
            ExceptionWriter.write(errorPointer, e);
            return CTypeConversion.toCBoolean(false);
        }
    }
}
