package com.amazonaws.services.schemaregistry;

import com.amazonaws.services.schemaregistry.config.NativeGlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;


/**
 * Initializes a singleton instance of a de-serializer used for subsequent invocations of decode.
 */
public class DeserializerInstance {
    private static GlueSchemaRegistryDeserializer instance = null;

    public static void create(NativeGlueSchemaRegistryConfiguration configuration) {
        instance = new GlueSchemaRegistryDeserializerImpl(configuration.getAwsCredentialsProvider(), configuration);
    }

    public static GlueSchemaRegistryDeserializer get() {
        if (instance == null) {
            throw new IllegalStateException("Deserializer is not initialized.");
        }
        return instance;
    }
}