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

@Slf4j
public final class AWSSchemaRegistryDeserializerCache implements AWSCache<UUID, Schema, CacheStats> {

    private static GlueSchemaRegistryConfiguration serDeConfigs = null;
    private Cache<UUID, Schema> cache;

    /**
     * Singleton cache.
     */
    private AWSSchemaRegistryDeserializerCache() { }

    /**
     * Private cache constructor.
     *
     * @param cache Cache instance
     */
    private AWSSchemaRegistryDeserializerCache(Cache<UUID, Schema> cache) {
        this.cache = cache;
    }

    /**
     * Helper method to create cache object
     *
     * @return AWSSchemaRegistryDeserializerCache instance with cache object
     */
    private static AWSSchemaRegistryDeserializerCache createDeserializerCache() {
        return new AWSSchemaRegistryDeserializerCache(CacheBuilder
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
     * @return returns a singleton instance for AWSSchemaRegistryDeserializerCache
     */
    public static AWSSchemaRegistryDeserializerCache getInstance(@NonNull GlueSchemaRegistryConfiguration configs) {
        serDeConfigs = configs;
        return CacheSingletonHelper.INSTANCE;
    }

    @Override
    public Schema get(UUID key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void put(UUID key, Schema value) {
        log.debug("Associating key {} with value {}", key, value);
        cache.put(key, value);
    }

    @Override
    public void delete(UUID key) {
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
        private static final AWSSchemaRegistryDeserializerCache INSTANCE = initialize();

        private static AWSSchemaRegistryDeserializerCache initialize() {
            return createDeserializerCache();
        }
    }

    private static class AWSCacheRemovalListener implements RemovalListener<UUID, Schema> {
        @Override
        public void onRemoval(RemovalNotification<UUID, Schema> notification) {
            log.debug("Removed key {} and value {} from cache cause {}", notification.getKey(), notification.getValue(),
                    notification.getCause());
        }
    }
}
