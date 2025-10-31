package com.amazonaws.services.schemaregistry.serializer;

import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufWireFormatEncoder;
import com.amazonaws.services.schemaregistry.utils.ProtobufSchemaParser;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

public class ProtobufPreprocessor {
    private static final ProtobufWireFormatEncoder ENCODER = new ProtobufWireFormatEncoder(new MessageIndexFinder());
    private static final MessageIndexFinder MESSAGE_INDEX_FINDER = new MessageIndexFinder();

    private static volatile long cacheLimit = 200; // same default as GlueSchemaRegistryConfiguration
    private static volatile LoadingCache<String, Descriptors.FileDescriptor> schemaDefToFileDescriptorCache;
    /*
     * CacheBuilder.newBuilder()
     * .maximumSize()
     * .refreshAfterWrite(glueSchemaRegistryConfiguration.getTimeToLiveMillis(),
     * TimeUnit.MILLISECONDS)
     * .build(new SchemaDefinitionToVersionCache());
     */

    public static String convertBase64SchemaToStringSchema(String base64Schema) throws InvalidProtocolBufferException {
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = DescriptorProtos.FileDescriptorProto
                .parseFrom(Base64.getDecoder().decode(base64Schema));

        return ProtobufSchemaParser.getProtobufSchemaStringFromFileDescriptorProto(fileDescriptorProto);
    }

    public static byte[] prefixMessageIndexToBytes(byte[] bytesToEncode, String schemaDef, String descriptorFullname)
            throws Descriptors.DescriptorValidationException, IOException, NoSuchMethodException, UncheckedExecutionException {

        Descriptors.FileDescriptor fileDescriptor = getFromCache(schemaDef);
        BiMap<Descriptors.Descriptor, Integer> allDescriptor = MESSAGE_INDEX_FINDER.getAll(fileDescriptor);

        for (Descriptors.Descriptor descriptor : allDescriptor.keySet()) {
            if (descriptor.getFullName().equals(descriptorFullname)) {
                return ENCODER.prefixMessageIndexToBytes(bytesToEncode, fileDescriptor, descriptor);
            }
        }
        return bytesToEncode;
    }

    /**
     * This method retrieves the file descriptor from the cache if the schema has
     * been processed before.
     * Additionally if the cache has not been initiated yet it will initiate the
     * cache with the static cache limit,
     * and then retrieve the file descriptor.
     * 
     * If a schema definition has not been seen yet it will calculate the file
     * descriptor, add it to the cache and then
     * return the result
     * 
     * @param schemaDef String schema definition for protobuf schema
     * @throws ExecutionException
     */
    private static Descriptors.FileDescriptor getFromCache(@Nonnull String schemaDef) throws UncheckedExecutionException {

        if (schemaDefToFileDescriptorCache == null) {
            synchronized (ProtobufPreprocessor.class) {
                if (schemaDefToFileDescriptorCache == null) {
                    initializeCache(cacheLimit);
                }
            }
        }
        return schemaDefToFileDescriptorCache.getUnchecked(schemaDef);
    }

    public static synchronized void initializeCache(long cacheSize) {
        cacheLimit = cacheSize;
        schemaDefToFileDescriptorCache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .recordStats()
                .build(new SchemaDefinitionToFileDescriptorCache());
    }

    @RequiredArgsConstructor
    private static class SchemaDefinitionToFileDescriptorCache extends CacheLoader<String, Descriptors.FileDescriptor> {
        @Override
        public FileDescriptor load(@Nonnull final String schemaDef) throws DescriptorValidationException {
            Descriptors.FileDescriptor fileDescriptor = com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufSchemaParser
                    .parse(schemaDef,
                            "any-name.proto");
            return fileDescriptor;
        }

    }
}
