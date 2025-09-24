package com.amazonaws.services.schemaregistry.serializer;

import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import com.google.common.util.concurrent.UncheckedExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class ProtobufPreprocessorTest {

    private String basicSyntax2Schema;
    private String basicSyntax3Schema;
    private String invalidSchema;

    @BeforeEach
    public void setUp() throws Exception {
        // Reset static state before each test
        resetStaticFields();

        // Define test schemas as strings
        basicSyntax2Schema = "syntax = \"proto2\";\n\n" +
                "package com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic;\n\n" +
                "message Phone {\n" +
                "  required string model = 1;\n" +
                "  optional string name = 2;\n" +
                "  optional int32 serial = 3;\n" +
                "}";

        basicSyntax3Schema = "syntax = \"proto3\";\n\n" +
                "package com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic;\n\n" +
                "message Phone {\n" +
                "  string model = 1;\n" +
                "  string name = 2;\n" +
                "  int32 serial = 3;\n" +
                "}";

        invalidSchema = "invalid proto schema content";
    }

    private void resetStaticFields() throws Exception {
        // Use reflection to reset static fields
        Field cacheField = ProtobufPreprocessor.class
                .getDeclaredField("schemaDefToFileDescriptorCache");
        cacheField.setAccessible(true);
        cacheField.set(null, null);

        Field limitField = ProtobufPreprocessor.class
                .getDeclaredField("cacheLimit");
        limitField.setAccessible(true);
        limitField.set(null, 200L);
    }

    @Test
    public void testCacheHitWithSingleSchema() throws Exception {
        // Test 1: Cache hit with single schema
        byte[] testBytes = new byte[] { 1, 2, 3 };
        String descriptorName = "com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.Phone";

        // First call - cache miss, loads into cache
        byte[] result1 = ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax2Schema, descriptorName);

        // Get cache statistics after first call
        LoadingCache<String, Descriptors.FileDescriptor> cache = getCacheViaReflection();
        long firstMissCount = cache.stats().missCount();

        // Second call - should be cache hit
        byte[] result2 = ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax2Schema, descriptorName);

        // Verify cache hit occurred
        assertEquals(1, cache.stats().hitCount(), "Cache hit count should be 1");
        assertEquals(firstMissCount, cache.stats().missCount(), "Miss count should remain same");

        // Results should be consistent
        assertArrayEquals(result1, result2, "Results should be identical");
    }

    @Test
    public void testCacheHitWithTwoSchemas() throws Exception {
        // Test 2: Cache hit with 2 schemas
        byte[] testBytes = new byte[] { 1, 2, 3 };

        // Load first schema
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax2Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.Phone");

        // Load second schema
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax3Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Phone");

        LoadingCache<String, Descriptors.FileDescriptor> cache = getCacheViaReflection();

        // Both should be in cache
        assertEquals(2, cache.size(), "Cache should have 2 entries");

        // Access both again - should be cache hits
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax2Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.Phone");
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax3Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Phone");

        assertEquals(2, cache.stats().hitCount(), "Should have 2 cache hits");
    }

    @Test
    public void testCacheSizeZeroNoCacheHits() throws Exception {
        // Test 3: Initialize to zero, no cache hits
        ProtobufPreprocessor.initializeCache(0);

        byte[] testBytes = new byte[] { 1, 2, 3 };

        // First call
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax2Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.Phone");

        // Second call - should NOT be cached
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax2Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.Phone");

        LoadingCache<String, Descriptors.FileDescriptor> cache = getCacheViaReflection();

        // With size 0, nothing should be cached
        assertEquals(0, cache.size(), "Cache size should be 0");
        assertEquals(0, cache.stats().hitCount(), "No cache hits expected");
        assertEquals(2, cache.stats().missCount(), "All should be misses");
    }

    @Test
    public void testInvalidSchemaThrowsExpectedException() {
        // Test 4: Invalid schema throws expected exception
        byte[] testBytes = new byte[] { 1, 2, 3 };

        UncheckedExecutionException exception = assertThrows(
                UncheckedExecutionException.class,
                () -> ProtobufPreprocessor.prefixMessageIndexToBytes(
                        testBytes, invalidSchema, "some.descriptor"));
    }

    @Test
    public void testCacheLimitOneEvictsFirstEntry() throws Exception {
        // Test 5: Cache limit 1, eviction test
        ProtobufPreprocessor.initializeCache(1);

        byte[] testBytes = new byte[] { 1, 2, 3 };

        // Load first schema
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax2Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.Phone");

        LoadingCache<String, Descriptors.FileDescriptor> cache = getCacheViaReflection();
        assertEquals(1, cache.size(), "Cache should have 1 entry");

        // Load second schema - should evict first
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax3Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Phone");

        assertEquals(1, cache.size(), "Cache should still have 1 entry");

        // Try to access first schema again - should be a miss (was evicted)
        long missCountBefore = cache.stats().missCount();
        ProtobufPreprocessor.prefixMessageIndexToBytes(
                testBytes, basicSyntax2Schema,
                "com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.Phone");

        assertEquals(missCountBefore + 1, cache.stats().missCount(),
                "Should have one more miss");
    }

    @Test
    public void testThreadSafetyOfCacheInitialization() throws Exception {
        // Additional test for thread safety
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        boolean[] success = new boolean[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    byte[] testBytes = new byte[] { 1, 2, 3 };
                    ProtobufPreprocessor.prefixMessageIndexToBytes(
                            testBytes, basicSyntax2Schema,
                            "com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.Phone");
                    success[index] = true;
                } catch (Exception e) {
                    success[index] = false;
                }
            });
        }

        // Start all threads
        for (Thread t : threads) {
            t.start();
        }

        // Wait for all to complete
        for (Thread t : threads) {
            t.join();
        }

        // Verify all threads succeeded
        for (int i = 0; i < threadCount; i++) {
            assertTrue(success[i], "Thread " + i + " should have succeeded");
        }

        // Cache should be initialized only once
        LoadingCache<String, Descriptors.FileDescriptor> cache = getCacheViaReflection();
        assertNotNull(cache, "Cache should be initialized");
        assertTrue(cache.size() > 0, "Cache should contain the schema");
    }

    @Test
    public void testCacheInitializationWithCustomSize() throws Exception {
        // Test custom cache size initialization
        long customSize = 50;
        ProtobufPreprocessor.initializeCache(customSize);

        LoadingCache<String, Descriptors.FileDescriptor> cache = getCacheViaReflection();
        assertNotNull(cache, "Cache should be initialized");

        // Verify cache limit was updated
        Field limitField = ProtobufPreprocessor.class.getDeclaredField("cacheLimit");
        limitField.setAccessible(true);
        long actualLimit = (Long) limitField.get(null);
        assertEquals(customSize, actualLimit, "Cache limit should be updated to custom size");
    }

    // Helper method to access private cache field
    private LoadingCache<String, Descriptors.FileDescriptor> getCacheViaReflection()
            throws Exception {
        Field cacheField = ProtobufPreprocessor.class
                .getDeclaredField("schemaDefToFileDescriptorCache");
        cacheField.setAccessible(true);
        return (LoadingCache<String, Descriptors.FileDescriptor>) cacheField.get(null);
    }
}
