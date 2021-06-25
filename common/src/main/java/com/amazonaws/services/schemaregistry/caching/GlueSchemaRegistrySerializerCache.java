/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.caching;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * GlueSchemaRegistrySerializerCache is implementation for the AWS Cache.
 */
@Slf4j
public final class GlueSchemaRegistrySerializerCache implements GlueSchemaRegistryCache<Schema, UUID, CacheStats> {

    private static GlueSchemaRegistryConfiguration serDeConfigs = null;
    private Cache<Schema, UUID> cache;

    /**
     * Private constructor.
     */
    private GlueSchemaRegistrySerializerCache() { }

    /**
     * Private cache constructor.
     *
     * @param cache Cache instance
     */
    private GlueSchemaRegistrySerializerCache(Cache<Schema, UUID> cache) {
        this.cache = cache;
    }

    /**
     * Helper method to create cache object
     *
     * @return GlueSchemaRegistrySerializerCache instance with cache object
     */
    private static GlueSchemaRegistrySerializerCache createSerializerCache() {
        return new GlueSchemaRegistrySerializerCache(CacheBuilder
                .newBuilder()
                .maximumSize(serDeConfigs.getCacheSize())
                .expireAfterWrite(serDeConfigs.getTimeToLiveMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .removalListener(new AWSCacheRemovalListener())
                .build());
    }

    /**
     * Singleton instantiation helper.
     *
     * @param configs configuration elements
     * @return returns a singleton instance for GlueSchemaRegistrySerializerCache
     */
    public static GlueSchemaRegistrySerializerCache getInstance(@NonNull GlueSchemaRegistryConfiguration configs) {
        serDeConfigs = configs;
        return CacheSingletonHelper.INSTANCE;
    }

    @Override
    public UUID get(Schema key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void put(Schema key, UUID value) {
        log.debug("Associating key {} with value {}", key, value);
        cache.put(key, value);
    }

    @Override
    public void delete(Schema key) {
        cache.invalidate(key);
    }

    @Override
    public void flushCache() {
        cache.invalidateAll();
    }

    @Override
    public long getCacheSize() {
        return cache.size();
    }

    @Override
    public CacheStats getCacheStats() {
        return cache.stats();
    }

    /**
     * Singleton helper.
     */
    private static class CacheSingletonHelper {
        private static final GlueSchemaRegistrySerializerCache INSTANCE = initialize();

        private static GlueSchemaRegistrySerializerCache initialize() {
            return createSerializerCache();
        }
    }

    private static class AWSCacheRemovalListener implements RemovalListener<Schema, UUID> {
        @Override
        public void onRemoval(RemovalNotification<Schema, UUID> notification) {
            log.debug(" key {} and value {} from cache cause {}", notification.getKey(), notification.getValue(),
                    notification.getCause());
        }
    }
}
