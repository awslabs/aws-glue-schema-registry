package com.amazonaws.services.schemaregistry;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;


/**
 * Initializes a singleton instance of a de-serializer used for subsequent invocations of decode.
 */
public class DeserializerInstance {
    private static GlueSchemaRegistryDeserializer instance = null;

    public static void create(GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {
        //TODO: Evaluate if credentials need to be configurable.
        instance = new GlueSchemaRegistryDeserializerImpl(DefaultCredentialsProvider.builder().build(), glueSchemaRegistryConfiguration);
    }

    public static GlueSchemaRegistryDeserializer get() {
        if (instance == null) {
            throw new IllegalStateException("Deserializer is not initialized.");
        }
        return instance;
    }
}