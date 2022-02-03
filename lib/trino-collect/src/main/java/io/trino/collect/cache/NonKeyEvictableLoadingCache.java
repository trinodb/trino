/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.collect.cache;

import com.google.common.cache.LoadingCache;

/**
 * A {@link com.google.common.cache.LoadingCache} that does not support key-based eviction.
 */
public interface NonKeyEvictableLoadingCache<K, V>
        extends LoadingCache<K, V>
{
    /**
     * @deprecated Not supported. Use {@link EvictableLoadingCache} cache implementation instead.
     */
    @Deprecated
    @Override
    void invalidate(Object key);

    /**
     * @deprecated Not supported. Use {@link EvictableLoadingCache} cache implementation instead.
     */
    @Deprecated
    @Override
    void invalidateAll(Iterable<?> keys);

    /**
     * Invalidates all live entries in the cache. Ongoing loads may not be invalidated, so subsequent
     * get from the cache is not guaranteed to return fresh state. Must not be relied on for correctness,
     * but can be used for manual intervention, e.g. as a method exposed over JMX.
     */
    @Override
    void invalidateAll();
}
