package com.amazonaws.services.schemaregistry;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * Initializes a singleton instance of a serializer used for subsequent invocations of encode.
 */
public class SerializerInstance {
    private static GlueSchemaRegistrySerializer instance = null;

    public static void create(GlueSchemaRegistryConfiguration configuration) {
        //TODO: Evaluate if credentials need to be configurable.
        instance = new GlueSchemaRegistrySerializerImpl(DefaultCredentialsProvider.builder().build(), configuration);
    }

    public static GlueSchemaRegistrySerializer get() {
        if (instance == null) {
            throw new IllegalStateException("Serializer is not initialized.");
        }
        return instance;
    }
}
