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

public interface AWSCache<K, V, Stats> {

    /**
     * Get the Value corresponding to the key.
     *
     * @param key key for cache entry
     * @return Value associated with the key. if the value is not present return
     * null.
     */
    V get(K key);

    /**
     * Put the key and value in the cache for subsequent use.
     *
     * @param key   key for cache entry
     * @param value value of the cache entry
     */
    void put(K key, V value);

    /**
     * Deletes the given element from the cache.
     *
     * @param key key for cache entry
     */
    void delete(K key);

    /**
     * Flushes the content of the cache.
     */
    void flushCache();

    /**
     * Get the current cache size.
     *
     * @return number of entries in the cache
     */
    long getCacheSize();

    /**
     * Get the cache stats. This will give the detailed view of the cache.
     *
     * @return stats object with cache statistic information
     */
    Stats getCacheStats();

}
