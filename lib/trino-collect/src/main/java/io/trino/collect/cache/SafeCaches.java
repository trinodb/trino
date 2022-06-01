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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

/**
 * @see EvictableCache
 * @see EvictableCacheBuilder
 */
public final class SafeCaches
{
    private SafeCaches() {}

    public static <K, V> NonEvictableCache<K, V> buildNonEvictableCache(CacheBuilder<? super K, ? super V> cacheBuilder)
    {
        return new NonEvictableCacheImpl<>(buildUnsafeCache(cacheBuilder));
    }

    /**
     * Builds a cache that supports {@link Cache#invalidateAll()} with best-effort semantics:
     * there is no guarantee that cache is empty after {@code invalidateAll()} returns, or that
     * subsequent read will not see stale state.
     */
    public static <K, V> NonKeyEvictableCache<K, V> buildNonEvictableCacheWithWeakInvalidateAll(CacheBuilder<? super K, ? super V> cacheBuilder)
    {
        return new NonKeyEvictableCacheImpl<>(buildUnsafeCache(cacheBuilder));
    }

    public static <K, V> NonEvictableLoadingCache<K, V> buildNonEvictableCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return new NonEvictableLoadingCacheImpl<>(buildUnsafeCache(cacheBuilder, cacheLoader));
    }

    /**
     * Builds a cache that supports {@link Cache#invalidateAll()} with best-effort semantics:
     * there is no guarantee that cache is empty after {@code invalidateAll()} returns, or that
     * subsequent read will not see stale state.
     */
    public static <K, V> NonKeyEvictableLoadingCache<K, V> buildNonEvictableCacheWithWeakInvalidateAll(
            CacheBuilder<? super K, ? super V> cacheBuilder,
            CacheLoader<? super K, V> cacheLoader)
    {
        return new NonKeyEvictableLoadingCacheImpl<>(buildUnsafeCache(cacheBuilder, cacheLoader));
    }

    @SuppressModernizer // CacheBuilder.build() is forbidden, advising to use this class as a safety-adding wrapper.
    private static <K, V> Cache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder)
    {
        return cacheBuilder.build();
    }

    @SuppressModernizer // CacheBuilder.build(CacheLoader) is forbidden, advising to use this class as a safety-adding wrapper.
    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return cacheBuilder.build(cacheLoader);
    }
}
